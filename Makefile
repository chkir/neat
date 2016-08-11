LIBOSMIUM_INCLUDE=/home/matt/Programming/Personal/libosmium/include
CAPNP_INCLUDE=/home/matt/include

CXX=g++
CXXFLAGS=-std=c++11
CAPNP=capnp
INCLUDE=-I$(LIBOSMIUM_INCLUDE) -I$(CAPNP_INCLUDE)
LDFLAGS=-pthread -lexpat -lbz2 -lz -lleveldb -L$(HOME)/lib -lcapnp -lkj

all: neat4

neat4: neat4.o neat4.capnp.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ -c $<

%.o: %.c++
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ -c $<

%.capnp.c++: %.capnp
	$(CAPNP) compile -oc++ $<

neat4.o: neat4.capnp.c++

# silence automatic rule that things the capnp file is an executable
neat4.capnp:;
