@[Link("libnghttp2")]
lib LibNghttp2
  # Opaque session type
  type Session = Void
  type SessionCallbacks = Void

  # ---- Constants ----

  # Frame flags
  FLAG_NONE        = 0x00_u8
  FLAG_END_STREAM  = 0x01_u8
  FLAG_END_HEADERS = 0x04_u8
  FLAG_ACK         = 0x01_u8
  FLAG_PADDED      = 0x08_u8
  FLAG_PRIORITY    = 0x20_u8

  # Frame types
  FRAME_DATA          = 0x0_u8
  FRAME_HEADERS       = 0x1_u8
  FRAME_PRIORITY      = 0x2_u8
  FRAME_RST_STREAM    = 0x3_u8
  FRAME_SETTINGS      = 0x4_u8
  FRAME_PUSH_PROMISE  = 0x5_u8
  FRAME_PING          = 0x6_u8
  FRAME_GOAWAY        = 0x7_u8
  FRAME_WINDOW_UPDATE = 0x8_u8
  FRAME_CONTINUATION  = 0x9_u8

  # Settings IDs
  SETTINGS_HEADER_TABLE_SIZE      = 0x01_i32
  SETTINGS_ENABLE_PUSH            = 0x02_i32
  SETTINGS_MAX_CONCURRENT_STREAMS = 0x03_i32
  SETTINGS_INITIAL_WINDOW_SIZE    = 0x04_i32
  SETTINGS_MAX_FRAME_SIZE         = 0x05_i32
  SETTINGS_MAX_HEADER_LIST_SIZE   = 0x06_i32

  # nv flags
  NV_FLAG_NONE          = 0x00_u8
  NV_FLAG_NO_INDEX      = 0x01_u8
  NV_FLAG_NO_COPY_NAME  = 0x02_u8
  NV_FLAG_NO_COPY_VALUE = 0x04_u8

  # data flags (used in read_callback via data_flags output parameter)
  DATA_FLAG_NONE          = 0x00_u32
  DATA_FLAG_EOF           = 0x01_u32
  DATA_FLAG_NO_END_STREAM = 0x02_u32
  DATA_FLAG_NO_COPY       = 0x04_u32

  # Error codes
  ERR_CALLBACK_FAILURE          = -902
  ERR_TEMPORAL_CALLBACK_FAILURE = -521
  ERR_INVALID_ARGUMENT          = -501
  ERR_WOULDBLOCK                = -504
  ERR_DEFERRED                  = -508

  # Error codes for RST_STREAM / GOAWAY
  NO_ERROR       = 0_u32
  NGHTTP2_CANCEL = 8_u32

  # ---- Structs ----

  # nghttp2_frame_hd: 8 + 4 + 1 + 1 + 1 = 15, padded to 16 bytes
  struct FrameHd
    length : LibC::SizeT # offset 0, 8 bytes
    stream_id : Int32    # offset 8, 4 bytes
    type : UInt8         # offset 12, 1 byte
    flags : UInt8        # offset 13, 1 byte
    reserved : UInt8     # offset 14, 1 byte
    # 1 byte padding to reach 16
  end

  # nghttp2_nv: 8 + 8 + 8 + 8 + 1 = 33, padded to 40 bytes
  struct Nv
    name : UInt8*          # offset 0
    value : UInt8*         # offset 8
    namelen : LibC::SizeT  # offset 16
    valuelen : LibC::SizeT # offset 24
    flags : UInt8          # offset 32
    # 7 bytes padding
  end

  # nghttp2_settings_entry: 4 + 4 = 8 bytes
  struct SettingsEntry
    settings_id : Int32
    value : UInt32
  end

  # nghttp2_data_source (union): max(4, 8) = 8 bytes
  union DataSource
    fd : Int32
    ptr : Void*
  end

  # nghttp2_data_provider: 8 + 8 = 16 bytes
  struct DataProvider
    source : DataSource
    read_callback : DataSourceReadCallback
  end

  # ---- Callback type aliases ----

  # ssize_t send_callback(session, data, length, flags, user_data)
  alias SendCallback = (Session*, UInt8*, LibC::SizeT, Int32, Void*) -> LibC::SSizeT

  # int on_frame_recv_callback(session, frame, user_data)
  alias OnFrameRecvCallback = (Session*, Void*, Void*) -> Int32

  # int on_frame_send_callback(session, frame, user_data)
  alias OnFrameSendCallback = (Session*, Void*, Void*) -> Int32

  # int on_begin_headers_callback(session, frame, user_data)
  alias OnBeginHeadersCallback = (Session*, Void*, Void*) -> Int32

  # int on_header_callback(session, frame, name, namelen, value, valuelen, flags, user_data)
  alias OnHeaderCallback = (Session*, Void*, UInt8*, LibC::SizeT, UInt8*, LibC::SizeT, UInt8, Void*) -> Int32

  # int on_data_chunk_recv_callback(session, flags, stream_id, data, len, user_data)
  alias OnDataChunkRecvCallback = (Session*, UInt8, Int32, UInt8*, LibC::SizeT, Void*) -> Int32

  # int on_stream_close_callback(session, stream_id, error_code, user_data)
  alias OnStreamCloseCallback = (Session*, Int32, UInt32, Void*) -> Int32

  # ssize_t data_source_read_callback(session, stream_id, buf, length, data_flags, source, user_data)
  alias DataSourceReadCallback = (Session*, Int32, UInt8*, LibC::SizeT, UInt32*, DataSource*, Void*) -> LibC::SSizeT

  # ---- Session Callbacks API ----

  fun session_callbacks_new = nghttp2_session_callbacks_new(
    callbacks_ptr : SessionCallbacks**,
  ) : Int32

  fun session_callbacks_del = nghttp2_session_callbacks_del(
    callbacks : SessionCallbacks*,
  ) : Void

  fun session_callbacks_set_send_callback = nghttp2_session_callbacks_set_send_callback(
    cbs : SessionCallbacks*, callback : SendCallback,
  ) : Void

  fun session_callbacks_set_on_frame_recv_callback = nghttp2_session_callbacks_set_on_frame_recv_callback(
    cbs : SessionCallbacks*, callback : OnFrameRecvCallback,
  ) : Void

  fun session_callbacks_set_on_frame_send_callback = nghttp2_session_callbacks_set_on_frame_send_callback(
    cbs : SessionCallbacks*, callback : OnFrameSendCallback,
  ) : Void

  fun session_callbacks_set_on_begin_headers_callback = nghttp2_session_callbacks_set_on_begin_headers_callback(
    cbs : SessionCallbacks*, callback : OnBeginHeadersCallback,
  ) : Void

  fun session_callbacks_set_on_header_callback = nghttp2_session_callbacks_set_on_header_callback(
    cbs : SessionCallbacks*, callback : OnHeaderCallback,
  ) : Void

  fun session_callbacks_set_on_data_chunk_recv_callback = nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
    cbs : SessionCallbacks*, callback : OnDataChunkRecvCallback,
  ) : Void

  fun session_callbacks_set_on_stream_close_callback = nghttp2_session_callbacks_set_on_stream_close_callback(
    cbs : SessionCallbacks*, callback : OnStreamCloseCallback,
  ) : Void

  # ---- Session creation / deletion ----

  fun session_server_new = nghttp2_session_server_new(
    session_ptr : Session**, callbacks : SessionCallbacks*, user_data : Void*,
  ) : Int32

  fun session_client_new = nghttp2_session_client_new(
    session_ptr : Session**, callbacks : SessionCallbacks*, user_data : Void*,
  ) : Int32

  fun session_del = nghttp2_session_del(session : Session*) : Void

  # ---- Session I/O ----

  fun session_mem_recv = nghttp2_session_mem_recv(
    session : Session*, data : UInt8*, datalen : LibC::SizeT,
  ) : LibC::SSizeT

  fun session_mem_send = nghttp2_session_mem_send(
    session : Session*, data_ptr : UInt8**,
  ) : LibC::SSizeT

  fun session_want_read = nghttp2_session_want_read(session : Session*) : Int32
  fun session_want_write = nghttp2_session_want_write(session : Session*) : Int32

  # ---- Submit functions ----

  fun submit_settings = nghttp2_submit_settings(
    session : Session*, flags : UInt8, iv : SettingsEntry*, niv : LibC::SizeT,
  ) : Int32

  fun submit_request = nghttp2_submit_request(
    session : Session*, pri_spec : Void*, nva : Nv*, nvlen : LibC::SizeT,
    data_prd : DataProvider*, stream_user_data : Void*,
  ) : Int32

  fun submit_response = nghttp2_submit_response(
    session : Session*, stream_id : Int32, nva : Nv*, nvlen : LibC::SizeT,
    data_prd : DataProvider*,
  ) : Int32

  fun submit_trailer = nghttp2_submit_trailer(
    session : Session*, stream_id : Int32, nva : Nv*, nvlen : LibC::SizeT,
  ) : Int32

  fun submit_headers = nghttp2_submit_headers(
    session : Session*, flags : UInt8, stream_id : Int32, pri_spec : Void*,
    nva : Nv*, nvlen : LibC::SizeT, stream_user_data : Void*,
  ) : Int32

  fun submit_data = nghttp2_submit_data(
    session : Session*, flags : UInt8, stream_id : Int32, data_prd : DataProvider*,
  ) : Int32

  fun submit_rst_stream = nghttp2_submit_rst_stream(
    session : Session*, flags : UInt8, stream_id : Int32, error_code : UInt32,
  ) : Int32

  fun submit_goaway = nghttp2_submit_goaway(
    session : Session*, flags : UInt8, last_stream_id : Int32,
    error_code : UInt32, opaque_data : UInt8*, opaque_data_len : LibC::SizeT,
  ) : Int32

  fun submit_window_update = nghttp2_submit_window_update(
    session : Session*, flags : UInt8, stream_id : Int32,
    window_size_increment : Int32,
  ) : Int32

  # ---- Stream user data ----

  fun session_set_stream_user_data = nghttp2_session_set_stream_user_data(
    session : Session*, stream_id : Int32, stream_user_data : Void*,
  ) : Int32

  fun session_get_stream_user_data = nghttp2_session_get_stream_user_data(
    session : Session*, stream_id : Int32,
  ) : Void*

  # ---- Utilities ----

  fun strerror = nghttp2_strerror(error_code : Int32) : UInt8*

  # session_resume_data resumes deferred DATA transmission for a stream.
  # Call this after pushing new data into the LiveSendBuffer.
  fun session_resume_data = nghttp2_session_resume_data(
    session : Session*, stream_id : Int32,
  ) : Int32
end
