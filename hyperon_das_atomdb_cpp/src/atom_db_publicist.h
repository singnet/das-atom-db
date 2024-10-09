#pragma once

#include "database.h"

using namespace atomdb;

class AtomDBPublicist : public AtomDB {
   public:
    using AtomDB::_build_link;
    using AtomDB::_build_node;
    using AtomDB::_get_atom;
};
