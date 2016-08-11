#include <osmium/io/any_input.hpp>
#include <osmium/io/input_iterator.hpp>
#include <osmium/handler.hpp>
#include <osmium/visitor.hpp>

#include <leveldb/db.h>

#include "neat4.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include <stdexcept>
#include <iostream>
#include <cassert>

void serialize_node(const osmium::Node &n,
                    ::capnp::MallocMessageBuilder &message) {
  Node::Builder node = message.initRoot<Node>();

  // TODO: handle invalid coordinates
  node.setLon(n.location().x());
  node.setLat(n.location().y());
  Common::Builder common = node.initCommon();
  common.setDeleted(n.deleted());
  common.setTimestamp(n.timestamp().seconds_since_epoch());
  if (n.user_is_anonymous()) {
    common.getUid().setAnonymous();
  } else {
    assert(n.uid() > 0);
    common.getUid().setUser(n.uid());
  }
  common.setChangeset(n.changeset());

  const size_t num_tags = n.tags().size();
  ::capnp::List<Tag>::Builder tags = common.initTags(num_tags);
  size_t i = 0;
  for (const auto &tag : n.tags()) {
    tags[i].setKey(tag.key());
    tags[i].setVal(tag.value());
    ++i;
  }
}

void mk_buffer(char type, uint64_t id, uint32_t v,
               char &buffer[sizeof(char) + sizeof(uint64_t) + sizeof(uint32_t)]) {
  buffer[0] = type;
  uint64_t *id_ptr = reinterpret_cast<uint64_t*>(&buffer[1]);
  uint32_t *v_ptr = reinterpret_cast<uint32_t*>(&buffer[1+sizeof(uint64_t)]);
  *id_ptr = htobe64(id);
  *v_ptr = htobe32(version);
}

class Database {
public:
  Database(std::string path) {
    leveldb::DB *db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    assert(status.ok());
    m_db.reset(db);
  }

  void insert_node(const osmium::Node &n) {
    assert(n.id() > 0);
    uint64_t id(n.id());
    uint32_t version(n.version());

    char id_buffer[sizeof(char) + sizeof(uint64_t) + sizeof(uint32_t)];
    mk_buffer('n', id, v, id_buffer);

    ::capnp::MallocMessageBuilder message;
    serialize_node(n, message)

    kj::Array<capnp::word> words = capnp::messageToFlatArray(message);
    kj::ArrayPtr<kj::byte> bytes = words.asBytes();

    m_db->Put(leveldb::WriteOptions(),
              leveldb::Slice(id_buffer, sizeof id_buffer),
              leveldb::Slice((const char *)bytes.begin(), bytes.size()));
  }

  /// returns the set of tiles which contain all the versions of the node
  /// with the given id.
  ///
  /// use this to check if the new node being added is also in the same
  /// set of tiles. if it is, then only the node itself needs to be
  /// emitted. if not, then the set needs to be enlarged, the new node
  /// added to all the old tiles, and all the nodes added to the new
  /// tile.
  std::set<tile_t> node_tiles(uint64_t id) {
    char start_buffer[sizeof(char) + sizeof(uint64_t) + sizeof(uint32_t)];
    char end_buffer[sizeof(char) + sizeof(uint64_t) + sizeof(uint32_t)];
    mk_buffer('n', id, 0, start_buffer);
    mk_buffer('n', id+1, 0, end_buffer);
    leveldb::Slice begin(start_buffer, sizeof start_buffer);
    leveldb::Slice end(end_buffer, sizeof end_buffer);

    std::set<tile_t> tiles;
    std::unique_ptr it(m_db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(begin); it->Valid() && it->key() < end; it->Next()) {
      tile_t *tile = (tile_t *)it->value();
      tiles.add(*tile);
    }

    return tiles;
  }

private:
  std::unique_ptr<leveldb::DB> m_db;
};

class Handler : public osmium::handler::Handler {
public:
  Handler(Database &db) : m_db(db) {}

  void node(const osmium::Node &n) const {
    m_db.insert(n);
  }

private:
  Database &m_db;
};

int main(int argc, char *argv[]) {
  assert(argc >= 2);

  try {
    osmium::io::File infile(argv[1]);
    osmium::io::Reader reader(infile);
    osmium::io::Header header = reader.header();

    Database db("db");

    osmium::io::InputIterator<osmium::io::Reader> itr(reader);
    const osmium::io::InputIterator<osmium::io::Reader> end;

    Handler handler(db);
    osmium::apply(itr, end, handler);

  } catch (const std::exception &e) {
    std::cerr << "FAIL: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
