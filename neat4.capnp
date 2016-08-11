@0xdcbb59667bc3f351;

struct Tag {
  key @0 :Text;
  val @1 :Text;
}

struct Common {
  deleted @0 :Bool;
  timestamp @1 :UInt64;
  uid :union {
    anonymous @2 :Void;
    user @3 :Int32;
  }
  changeset @4 :UInt64;
  tags @5 :List(Tag);
}

struct Node {
  lon @0 :Int32;
  lat @1 :Int32;
  common @2 :Common;
}
