g++ -std=c++20 -g main.cpp codec/codec_int.cpp codec/codec_string.cpp serialization/serializator.cpp serialization/deserializator.cpp validation/validator.cpp -I zstd/lib -L zstd/lib -lzstd -o main
