dorb [![Build Status](https://travis-ci.org/omarkj/dorb.svg)](https://travis-ci.org/omarkj/dorb)
=====

## Known issues

* When a messageset is produced with the `RequiredAcks` set at 0, the waiting
  client will time out (if called with `send_sync`). This will also leak
  `#waiter{}` records in `dorb_socket`;
* CRC32 validation is not done on incoming messages;
* If a `dorb_socket` process crashes, it will leave callers waiting until they
  time out. (Fix is to monitor the `dorb_socket` process and handle the `DOWN`
  message gracefully - monitoring also returns a `ref` that could be reused).