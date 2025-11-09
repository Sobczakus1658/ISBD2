CXX = g++
CXXFLAGS = -std=c++20 -g -I zstd/lib
LDFLAGS = -L zstd/lib -lzstd
TARGET = main

SRC = main.cpp \
      tests/tests.cpp \
      codec/codec_int.cpp \
      codec/codec_string.cpp \
      serialization/serializator.cpp \
      serialization/deserializator.cpp \
      validation/validator.cpp \
      statistics/statistics.cpp

OBJ = $(SRC:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean
