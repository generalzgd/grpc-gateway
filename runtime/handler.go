package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/internal"
	"google.golang.org/grpc/grpclog"
)

var errEmptyResponse = errors.New("empty response")

// defaultDelimiter 是非 SSE 流式响应的默认帧分隔符。
var defaultDelimiter = []byte("\n")

// sseContentType 是流式响应统一使用的 Content-Type，标识这是标准 SSE 帧。
const sseContentType = "text/event-stream; charset=utf-8"

// 后端通过响应头 metadata 声明“服务端流式”的标识：
//	gRPC 服务端：header := metadata.Pairs("X-Stream-Type", "server-streaming"); stream.SendHeader(header)
// 只有携带该标识的响应才输出标准 SSE 帧，其余保持 grpc-gateway 原有行为。
const (
	xStreamTypeKey   = "X-Stream-Type"
	xStreamTypeValue = "server-streaming"
)

// streamFrame 封装一次流式转发的帧格式：是否走标准 SSE，以及非 SSE 时的分隔符。
// SSE 模式由后端通过响应头 metadata 显式声明（见 newStreamFrame）。
type streamFrame struct {
	sse       bool
	delimiter []byte
}

// newStreamFrame 根据响应头决定帧格式：只要存在 “流式” 标识
// （X-Stream-Type: server-streaming，键名不区分大小写），就使用标准 SSE 帧
// （data: {json}\n\n，Content-Type: text/event-stream）；
// 否则保持 grpc-gateway 原有行为（裸 JSON + 换行符分隔）。
func newStreamFrame(ctx context.Context, w http.ResponseWriter, marshaler Marshaler) streamFrame {
	if isServerStreaming(ctx, w) {
		return streamFrame{sse: true}
	}
	delimiter := defaultDelimiter
	if d, ok := marshaler.(Delimited); ok {
		delimiter = d.Delimiter()
	}
	return streamFrame{delimiter: delimiter}
}

// isServerStreaming 判断后端是否声明了服务端流式。
// 同时检查两处来源：gRPC 响应头 metadata（HeaderMD）与已写出的 HTTP 响应头，
// 二者任一命中即可（网关的 OutgoingHeaderMatcher 可能已改写键名）。
func isServerStreaming(ctx context.Context, w http.ResponseWriter) bool {
	if md, ok := ServerMetadataFromContext(ctx); ok {
		for _, v := range md.HeaderMD.Get(xStreamTypeKey) {
			if strings.EqualFold(v, xStreamTypeValue) {
				return true
			}
		}
	}
	if w != nil {
		if strings.EqualFold(w.Header().Get(xStreamTypeKey), xStreamTypeValue) {
			return true
		}
	}
	return false
}

// writeFrame 按帧格式写出一条流式消息。SSE 下写成 `data: <payload>\n\n`；
// 否则写出 payload 本身并追加分隔符。
func writeFrame(w http.ResponseWriter, payload []byte, frame streamFrame) error {
	if frame.sse {
		return writeSSEChunk(w, payload)
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.Write(frame.delimiter)
	return err
}

// writeSSEChunk 把一个已序列化的数据块写成一条标准 SSE 消息：`data: <payload>\n\n`。
// payload 内部若含换行，会按 SSE 规范拆成多行 data:。
func writeSSEChunk(w http.ResponseWriter, payload []byte) error {
	for _, ln := range bytes.Split(payload, []byte("\n")) {
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(ln); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	// 空行结束一条 SSE 消息
	_, err := w.Write([]byte("\n"))
	return err
}

// setStreamResponseHeader 按帧格式设置响应头。SSE 模式设置标准 SSE 头；
// 非 SSE 保持原有 chunked + marshaler Content-Type。
func setStreamResponseHeader(w http.ResponseWriter, marshaler Marshaler, frame streamFrame) {
	if frame.sse {
		w.Header().Set("Content-Type", sseContentType)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// 禁用 Nginx 等反向代理的缓冲，确保数据实时到达客户端
		w.Header().Set("X-Accel-Buffering", "no")
		return
	}
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Content-Type", marshaler.ContentType())
}

// ForwardResponseStream forwards the stream from gRPC server to REST client.
func ForwardResponseStream(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, recv func() (proto.Message, error), opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	f, ok := w.(http.Flusher)
	if !ok {
		grpclog.Infof("Flush not supported in %T", w)
		http.Error(w, "unexpected type of web server", http.StatusInternalServerError)
		return
	}

	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Infof("Failed to extract ServerMetadata from context")
		http.Error(w, "unexpected error", http.StatusInternalServerError)
		return
	}
	handleForwardResponseServerMetadata(w, mux, md)

	frame := newStreamFrame(ctx, w, marshaler)
	setStreamResponseHeader(w, marshaler, frame)
	if err := handleForwardResponseOptions(ctx, w, nil, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	var wroteHeader bool
	for {
		resp, err := recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}
		if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}

		var buf []byte
		switch {
		case resp == nil:
			buf, err = marshaler.Marshal(errorChunk(streamError(ctx, mux.streamErrorHandler, errEmptyResponse)))
		default:
			result := map[string]interface{}{"result": resp}
			if rb, ok := resp.(responseBody); ok {
				result["result"] = rb.XXX_ResponseBody()
			}

			buf, err = marshaler.Marshal(result)
		}

		if err != nil {
			grpclog.Infof("Failed to marshal response chunk: %v", err)
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}
		if err = writeFrame(w, buf, frame); err != nil {
			grpclog.Infof("Failed to send response chunk: %v", err)
			return
		}
		wroteHeader = true
		f.Flush()
	}
}

// ForwardResponseStreamGoGo forwards the stream from gRPC server to REST client.
func ForwardResponseStreamGoGo(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, recv func() (gogoproto.Message, error), opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	f, ok := w.(http.Flusher)
	if !ok {
		grpclog.Infof("Flush not supported in %T", w)
		http.Error(w, "unexpected type of web server", http.StatusInternalServerError)
		return
	}

	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Infof("Failed to extract ServerMetadata from context")
		http.Error(w, "unexpected error", http.StatusInternalServerError)
		return
	}
	handleForwardResponseServerMetadata(w, mux, md)

	frame := newStreamFrame(ctx, w, marshaler)
	setStreamResponseHeader(w, marshaler, frame)
	if err := handleForwardResponseOptions(ctx, w, nil, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	var wroteHeader bool
	for {
		resp, err := recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}
		if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}

		var buf []byte
		switch {
		case resp == nil:
			buf, err = marshaler.Marshal(errorChunk(streamError(ctx, mux.streamErrorHandler, errEmptyResponse)))
		default:
			result := map[string]interface{}{"result": resp}
			if rb, ok := resp.(responseBody); ok {
				result["result"] = rb.XXX_ResponseBody()
			}

			buf, err = marshaler.Marshal(result)
		}

		if err != nil {
			grpclog.Infof("Failed to marshal response chunk: %v", err)
			handleForwardResponseStreamError(ctx, wroteHeader, marshaler, w, req, mux, err, frame)
			return
		}
		if err = writeFrame(w, buf, frame); err != nil {
			grpclog.Infof("Failed to send response chunk: %v", err)
			return
		}
		wroteHeader = true
		f.Flush()
	}
}

func handleForwardResponseServerMetadata(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k, vs := range md.HeaderMD {
		if h, ok := mux.outgoingHeaderMatcher(k); ok {
			for _, v := range vs {
				w.Header().Add(h, v)
			}
		}
	}
}

func handleForwardResponseTrailerHeader(w http.ResponseWriter, md ServerMetadata) {
	for k := range md.TrailerMD {
		tKey := textproto.CanonicalMIMEHeaderKey(fmt.Sprintf("%s%s", MetadataTrailerPrefix, k))
		w.Header().Add("Trailer", tKey)
	}
}

func handleForwardResponseTrailer(w http.ResponseWriter, md ServerMetadata) {
	for k, vs := range md.TrailerMD {
		tKey := fmt.Sprintf("%s%s", MetadataTrailerPrefix, k)
		for _, v := range vs {
			w.Header().Add(tKey, v)
		}
	}
}

// responseBody interface contains method for getting field for marshaling to the response body
// this method is generated for response struct from the value of `response_body` in the `google.api.HttpRule`
type responseBody interface {
	XXX_ResponseBody() interface{}
}

// ForwardResponseMessage forwards the message "resp" from gRPC server to REST client.
func ForwardResponseMessage(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Infof("Failed to extract ServerMetadata from context")
	}

	handleForwardResponseServerMetadata(w, mux, md)
	handleForwardResponseTrailerHeader(w, md)

	contentType := marshaler.ContentType()
	// Check marshaler on run time in order to keep backwards compatibility
	// An interface param needs to be added to the ContentType() function on
	// the Marshal interface to be able to remove this check
	if typeMarshaler, ok := marshaler.(contentTypeMarshaler); ok {
		contentType = typeMarshaler.ContentTypeFromMessage(resp)
	}
	w.Header().Set("Content-Type", contentType)

	if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}
	var buf []byte
	var err error
	if rb, ok := resp.(responseBody); ok {
		tmp := rb.XXX_ResponseBody()
		if mux.respContainMuter != nil {
			tmp = mux.respContainMuter(marshaler, tmp)
		}
		buf, err = marshaler.Marshal(tmp)
	} else {
		var tmp interface{}
		tmp = resp
		if mux.respContainMuter != nil {
			tmp = mux.respContainMuter(marshaler, tmp)
		}
		buf, err = marshaler.Marshal(tmp)
	}
	if err != nil {
		grpclog.Infof("Marshal error: %v", err)
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	if _, err = w.Write(buf); err != nil {
		grpclog.Infof("Failed to write response: %v", err)
	}

	handleForwardResponseTrailer(w, md)
}

func handleForwardResponseOptions(ctx context.Context, w http.ResponseWriter, resp proto.Message, opts []func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if len(opts) == 0 {
		return nil
	}
	for _, opt := range opts {
		if err := opt(ctx, w, resp); err != nil {
			grpclog.Infof("Error handling ForwardResponseOptions: %v", err)
			return err
		}
	}
	return nil
}

func handleForwardResponseStreamError(ctx context.Context, wroteHeader bool, marshaler Marshaler, w http.ResponseWriter, req *http.Request, mux *ServeMux, err error, frame streamFrame) {
	serr := streamError(ctx, mux.streamErrorHandler, err)
	if !wroteHeader {
		w.WriteHeader(int(serr.HttpCode))
	}
	buf, merr := marshaler.Marshal(errorChunk(serr))
	if merr != nil {
		grpclog.Infof("Failed to marshal an error: %v", merr)
		return
	}
	if werr := writeFrame(w, buf, frame); werr != nil {
		grpclog.Infof("Failed to notify error to client: %v", werr)
		return
	}
}

// streamError returns the payload for the final message in a response stream
// that represents the given err.
func streamError(ctx context.Context, errHandler StreamErrorHandlerFunc, err error) *StreamError {
	serr := errHandler(ctx, err)
	if serr != nil {
		return serr
	}
	// TODO: log about misbehaving stream error handler?
	return DefaultHTTPStreamErrorHandler(ctx, err)
}

func errorChunk(err *StreamError) map[string]proto.Message {
	return map[string]proto.Message{"error": (*internal.StreamError)(err)}
}
