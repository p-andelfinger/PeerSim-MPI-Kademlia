#------------------------------

TARGET=wrapper
SRC=mpi-wrapper.cc

CXX=mpic++
CCFLAGS=-O3
LDFLAGS=

OBJS=$(subst .cc,.o,$(SRC))

#------------------------------

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CCFLAGS) $(LDFLAGS) $(OBJS) -o $(TARGET)

clean:
	rm -f *.o $(TARGET)
