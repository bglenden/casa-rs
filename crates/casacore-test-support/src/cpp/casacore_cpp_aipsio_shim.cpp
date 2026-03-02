#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/casa/BasicSL/String.h>
#include <casacore/casa/IO/CanonicalIO.h>
#include <casacore/casa/IO/LECanonicalIO.h>
#include <casacore/casa/IO/MemoryIO.h>
#include <casacore/casa/aips.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

enum PrimitiveTag : std::uint8_t {
    TAG_BOOL = 0,
    TAG_INT16 = 1,
    TAG_INT32 = 2,
    TAG_INT64 = 3,
    TAG_FLOAT32 = 4,
    TAG_FLOAT64 = 5,
    TAG_COMPLEX32 = 6,
    TAG_COMPLEX64 = 7,
    TAG_STRING = 8,
};

enum ByteOrderTag : std::uint8_t {
    ORDER_BIG = 0,
    ORDER_LITTLE = 1,
};

std::shared_ptr<casacore::TypeIO> make_type_io(
    const std::shared_ptr<casacore::MemoryIO>& memory,
    std::uint8_t byte_order) {
    if (byte_order == ORDER_LITTLE) {
        return std::make_shared<casacore::LECanonicalIO>(memory);
    }
    return std::make_shared<casacore::CanonicalIO>(memory);
}

std::uint16_t read_u16_le(const std::uint8_t* data) {
    return static_cast<std::uint16_t>(data[0]) |
           (static_cast<std::uint16_t>(data[1]) << 8);
}

std::uint32_t read_u32_le(const std::uint8_t* data) {
    return static_cast<std::uint32_t>(data[0]) |
           (static_cast<std::uint32_t>(data[1]) << 8) |
           (static_cast<std::uint32_t>(data[2]) << 16) |
           (static_cast<std::uint32_t>(data[3]) << 24);
}

std::uint64_t read_u64_le(const std::uint8_t* data) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(data[i]) << (8 * i);
    }
    return value;
}

void push_u16_le(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
}

void push_u32_le(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xff));
}

void push_u64_le(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>((value >> (8 * i)) & 0xff));
    }
}

std::int16_t read_i16_le(const std::uint8_t* data) {
    return static_cast<std::int16_t>(read_u16_le(data));
}

std::int32_t read_i32_le(const std::uint8_t* data) {
    return static_cast<std::int32_t>(read_u32_le(data));
}

std::int64_t read_i64_le(const std::uint8_t* data) {
    return static_cast<std::int64_t>(read_u64_le(data));
}

float read_f32_le(const std::uint8_t* data) {
    const std::uint32_t bits = read_u32_le(data);
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

double read_f64_le(const std::uint8_t* data) {
    const std::uint64_t bits = read_u64_le(data);
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

void push_i16_le(std::vector<std::uint8_t>& out, std::int16_t value) {
    push_u16_le(out, static_cast<std::uint16_t>(value));
}

void push_i32_le(std::vector<std::uint8_t>& out, std::int32_t value) {
    push_u32_le(out, static_cast<std::uint32_t>(value));
}

void push_i64_le(std::vector<std::uint8_t>& out, std::int64_t value) {
    push_u64_le(out, static_cast<std::uint64_t>(value));
}

void push_f32_le(std::vector<std::uint8_t>& out, float value) {
    std::uint32_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    push_u32_le(out, bits);
}

void push_f64_le(std::vector<std::uint8_t>& out, double value) {
    std::uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    push_u64_le(out, bits);
}

std::uint32_t checked_u32(std::size_t value, const char* what) {
    if (value > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error(std::string(what) + " does not fit into u32");
    }
    return static_cast<std::uint32_t>(value);
}

std::size_t elem_width(std::uint8_t primitive) {
    switch (primitive) {
    case TAG_BOOL:
        return 1;
    case TAG_INT16:
        return 2;
    case TAG_INT32:
        return 4;
    case TAG_INT64:
        return 8;
    case TAG_FLOAT32:
        return 4;
    case TAG_FLOAT64:
        return 8;
    case TAG_COMPLEX32:
        return 8;
    case TAG_COMPLEX64:
        return 16;
    case TAG_STRING:
        return 0;
    default:
        throw std::runtime_error("unknown primitive tag");
    }
}

std::string make_error_message(const std::exception& ex) {
    return ex.what();
}

char* dup_error(const std::string& value) {
    const std::size_t n = value.size();
    auto* mem = static_cast<char*>(std::malloc(n + 1));
    if (!mem) {
        return nullptr;
    }
    std::memcpy(mem, value.data(), n);
    mem[n] = '\0';
    return mem;
}

void assign_bytes_out(const std::vector<std::uint8_t>& data, std::uint8_t** out_ptr, std::size_t* out_len) {
    auto* mem = static_cast<std::uint8_t*>(std::malloc(data.size()));
    if (data.size() > 0 && !mem) {
        throw std::bad_alloc();
    }
    if (!data.empty()) {
        std::memcpy(mem, data.data(), data.size());
    }
    *out_ptr = mem;
    *out_len = data.size();
}

void assign_offsets_out(const std::vector<std::uint32_t>& offsets, std::uint32_t** out_ptr, std::size_t* out_len) {
    auto* mem = static_cast<std::uint32_t*>(std::malloc(offsets.size() * sizeof(std::uint32_t)));
    if (!offsets.empty() && !mem) {
        throw std::bad_alloc();
    }
    if (!offsets.empty()) {
        std::memcpy(mem, offsets.data(), offsets.size() * sizeof(std::uint32_t));
    }
    *out_ptr = mem;
    *out_len = offsets.size();
}

} // namespace

extern "C" int casacore_cpp_aipsio_encode(
    std::uint8_t primitive,
    std::uint8_t is_array,
    std::uint8_t byte_order,
    const std::uint8_t* payload_ptr,
    std::size_t payload_len,
    const std::uint32_t* offsets_ptr,
    std::size_t offsets_len,
    std::uint8_t** out_wire_ptr,
    std::size_t* out_wire_len,
    char** out_error) {
    *out_wire_ptr = nullptr;
    *out_wire_len = 0;
    *out_error = nullptr;

    try {
        auto memory = std::make_shared<casacore::MemoryIO>(payload_len + 128, 1024);
        auto type_io = make_type_io(memory, byte_order);

        if (is_array) {
            std::uint32_t n = 0;
            if (primitive == TAG_STRING) {
                if (offsets_len == 0) {
                    throw std::runtime_error("string array offsets are required");
                }
                n = checked_u32(offsets_len - 1, "string array length");
            } else {
                const std::size_t width = elem_width(primitive);
                if (width == 0 || payload_len % width != 0) {
                    throw std::runtime_error("invalid payload length for primitive array");
                }
                n = checked_u32(payload_len / width, "array length");
            }
            type_io->write(1, &n);

            if (primitive == TAG_BOOL) {
                for (std::size_t i = 0; i < n; ++i) {
                    casacore::uChar b = payload_ptr[i] ? 1 : 0;
                    type_io->write(1, &b);
                }
            } else if (primitive == TAG_INT16) {
                std::vector<casacore::Short> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    values[i] = static_cast<casacore::Short>(read_i16_le(payload_ptr + i * 2));
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_INT32) {
                std::vector<casacore::Int> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    values[i] = static_cast<casacore::Int>(read_i32_le(payload_ptr + i * 4));
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_INT64) {
                std::vector<casacore::Int64> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    values[i] = static_cast<casacore::Int64>(read_i64_le(payload_ptr + i * 8));
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_FLOAT32) {
                std::vector<casacore::Float> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    values[i] = static_cast<casacore::Float>(read_f32_le(payload_ptr + i * 4));
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_FLOAT64) {
                std::vector<casacore::Double> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    values[i] = static_cast<casacore::Double>(read_f64_le(payload_ptr + i * 8));
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_COMPLEX32) {
                std::vector<casacore::Complex> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    const float re = read_f32_le(payload_ptr + i * 8);
                    const float im = read_f32_le(payload_ptr + i * 8 + 4);
                    values[i] = casacore::Complex(re, im);
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_COMPLEX64) {
                std::vector<casacore::DComplex> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    const double re = read_f64_le(payload_ptr + i * 16);
                    const double im = read_f64_le(payload_ptr + i * 16 + 8);
                    values[i] = casacore::DComplex(re, im);
                }
                type_io->write(n, values.data());
            } else if (primitive == TAG_STRING) {
                if (offsets_len < 1 || offsets_ptr[0] != 0) {
                    throw std::runtime_error("invalid string offsets");
                }
                std::vector<casacore::String> values(n);
                for (std::size_t i = 0; i < n; ++i) {
                    const std::size_t start = offsets_ptr[i];
                    const std::size_t end = offsets_ptr[i + 1];
                    if (start > end || end > payload_len) {
                        throw std::runtime_error("invalid string offset range");
                    }
                    values[i] = casacore::String(
                        reinterpret_cast<const char*>(payload_ptr + start),
                        checked_u32(end - start, "string length"));
                }
                type_io->write(n, values.data());
            } else {
                throw std::runtime_error("unknown primitive tag");
            }
        } else {
            if (primitive == TAG_BOOL) {
                if (payload_len != 1) {
                    throw std::runtime_error("bool scalar payload must be 1 byte");
                }
                casacore::uChar b = payload_ptr[0] ? 1 : 0;
                type_io->write(1, &b);
            } else if (primitive == TAG_INT16) {
                if (payload_len != 2) {
                    throw std::runtime_error("int16 scalar payload must be 2 bytes");
                }
                casacore::Short value = static_cast<casacore::Short>(read_i16_le(payload_ptr));
                type_io->write(1, &value);
            } else if (primitive == TAG_INT32) {
                if (payload_len != 4) {
                    throw std::runtime_error("int32 scalar payload must be 4 bytes");
                }
                casacore::Int value = static_cast<casacore::Int>(read_i32_le(payload_ptr));
                type_io->write(1, &value);
            } else if (primitive == TAG_INT64) {
                if (payload_len != 8) {
                    throw std::runtime_error("int64 scalar payload must be 8 bytes");
                }
                casacore::Int64 value = static_cast<casacore::Int64>(read_i64_le(payload_ptr));
                type_io->write(1, &value);
            } else if (primitive == TAG_FLOAT32) {
                if (payload_len != 4) {
                    throw std::runtime_error("float32 scalar payload must be 4 bytes");
                }
                casacore::Float value = static_cast<casacore::Float>(read_f32_le(payload_ptr));
                type_io->write(1, &value);
            } else if (primitive == TAG_FLOAT64) {
                if (payload_len != 8) {
                    throw std::runtime_error("float64 scalar payload must be 8 bytes");
                }
                casacore::Double value = static_cast<casacore::Double>(read_f64_le(payload_ptr));
                type_io->write(1, &value);
            } else if (primitive == TAG_COMPLEX32) {
                if (payload_len != 8) {
                    throw std::runtime_error("complex32 scalar payload must be 8 bytes");
                }
                casacore::Complex value(read_f32_le(payload_ptr), read_f32_le(payload_ptr + 4));
                type_io->write(1, &value);
            } else if (primitive == TAG_COMPLEX64) {
                if (payload_len != 16) {
                    throw std::runtime_error("complex64 scalar payload must be 16 bytes");
                }
                casacore::DComplex value(read_f64_le(payload_ptr), read_f64_le(payload_ptr + 8));
                type_io->write(1, &value);
            } else if (primitive == TAG_STRING) {
                casacore::String value(reinterpret_cast<const char*>(payload_ptr), checked_u32(payload_len, "string length"));
                type_io->write(1, &value);
            } else {
                throw std::runtime_error("unknown primitive tag");
            }
        }

        std::vector<std::uint8_t> out(memory->length());
        if (!out.empty()) {
            std::memcpy(out.data(), memory->getBuffer(), out.size());
        }
        assign_bytes_out(out, out_wire_ptr, out_wire_len);
        return 0;
    } catch (const std::exception& ex) {
        *out_error = dup_error(make_error_message(ex));
        return 1;
    }
}

extern "C" int casacore_cpp_aipsio_decode(
    std::uint8_t primitive,
    std::uint8_t is_array,
    std::uint8_t byte_order,
    const std::uint8_t* wire_ptr,
    std::size_t wire_len,
    std::uint8_t** out_payload_ptr,
    std::size_t* out_payload_len,
    std::uint32_t** out_offsets_ptr,
    std::size_t* out_offsets_len,
    char** out_error) {
    *out_payload_ptr = nullptr;
    *out_payload_len = 0;
    *out_offsets_ptr = nullptr;
    *out_offsets_len = 0;
    *out_error = nullptr;

    try {
        auto memory = std::make_shared<casacore::MemoryIO>(wire_ptr, wire_len);
        auto type_io = make_type_io(memory, byte_order);
        std::vector<std::uint8_t> payload;
        std::vector<std::uint32_t> offsets;

        if (is_array) {
            casacore::uInt n = 0;
            type_io->read(1, &n);

            if (primitive == TAG_BOOL) {
                payload.resize(n);
                for (std::size_t i = 0; i < n; ++i) {
                    casacore::uChar b = 0;
                    type_io->read(1, &b);
                    payload[i] = (b != 0) ? 1 : 0;
                }
            } else if (primitive == TAG_INT16) {
                std::vector<casacore::Short> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 2);
                for (casacore::Short value : values) {
                    push_i16_le(payload, static_cast<std::int16_t>(value));
                }
            } else if (primitive == TAG_INT32) {
                std::vector<casacore::Int> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 4);
                for (casacore::Int value : values) {
                    push_i32_le(payload, static_cast<std::int32_t>(value));
                }
            } else if (primitive == TAG_INT64) {
                std::vector<casacore::Int64> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 8);
                for (casacore::Int64 value : values) {
                    push_i64_le(payload, static_cast<std::int64_t>(value));
                }
            } else if (primitive == TAG_FLOAT32) {
                std::vector<casacore::Float> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 4);
                for (casacore::Float value : values) {
                    push_f32_le(payload, static_cast<float>(value));
                }
            } else if (primitive == TAG_FLOAT64) {
                std::vector<casacore::Double> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 8);
                for (casacore::Double value : values) {
                    push_f64_le(payload, static_cast<double>(value));
                }
            } else if (primitive == TAG_COMPLEX32) {
                std::vector<casacore::Complex> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 8);
                for (const casacore::Complex& value : values) {
                    push_f32_le(payload, value.real());
                    push_f32_le(payload, value.imag());
                }
            } else if (primitive == TAG_COMPLEX64) {
                std::vector<casacore::DComplex> values(n);
                type_io->read(n, values.data());
                payload.reserve(n * 16);
                for (const casacore::DComplex& value : values) {
                    push_f64_le(payload, value.real());
                    push_f64_le(payload, value.imag());
                }
            } else if (primitive == TAG_STRING) {
                std::vector<casacore::String> values(n);
                type_io->read(n, values.data());
                offsets.reserve(n + 1);
                offsets.push_back(0);
                std::uint32_t total = 0;
                for (const casacore::String& value : values) {
                    const std::size_t len = value.size();
                    const std::uint32_t len32 = checked_u32(len, "string length");
                    payload.insert(payload.end(), value.begin(), value.end());
                    total += len32;
                    offsets.push_back(total);
                }
            } else {
                throw std::runtime_error("unknown primitive tag");
            }
        } else {
            if (primitive == TAG_BOOL) {
                casacore::uChar value = 0;
                type_io->read(1, &value);
                payload.push_back((value != 0) ? 1 : 0);
            } else if (primitive == TAG_INT16) {
                casacore::Short value = 0;
                type_io->read(1, &value);
                push_i16_le(payload, static_cast<std::int16_t>(value));
            } else if (primitive == TAG_INT32) {
                casacore::Int value = 0;
                type_io->read(1, &value);
                push_i32_le(payload, static_cast<std::int32_t>(value));
            } else if (primitive == TAG_INT64) {
                casacore::Int64 value = 0;
                type_io->read(1, &value);
                push_i64_le(payload, static_cast<std::int64_t>(value));
            } else if (primitive == TAG_FLOAT32) {
                casacore::Float value = 0;
                type_io->read(1, &value);
                push_f32_le(payload, static_cast<float>(value));
            } else if (primitive == TAG_FLOAT64) {
                casacore::Double value = 0;
                type_io->read(1, &value);
                push_f64_le(payload, static_cast<double>(value));
            } else if (primitive == TAG_COMPLEX32) {
                casacore::Complex value;
                type_io->read(1, &value);
                push_f32_le(payload, value.real());
                push_f32_le(payload, value.imag());
            } else if (primitive == TAG_COMPLEX64) {
                casacore::DComplex value;
                type_io->read(1, &value);
                push_f64_le(payload, value.real());
                push_f64_le(payload, value.imag());
            } else if (primitive == TAG_STRING) {
                casacore::String value;
                type_io->read(1, &value);
                payload.insert(payload.end(), value.begin(), value.end());
                offsets.push_back(0);
                offsets.push_back(checked_u32(value.size(), "string length"));
            } else {
                throw std::runtime_error("unknown primitive tag");
            }
        }

        assign_bytes_out(payload, out_payload_ptr, out_payload_len);
        assign_offsets_out(offsets, out_offsets_ptr, out_offsets_len);
        return 0;
    } catch (const std::exception& ex) {
        *out_error = dup_error(make_error_message(ex));
        return 1;
    }
}

extern "C" void casacore_cpp_aipsio_free_bytes(std::uint8_t* ptr) {
    std::free(ptr);
}

extern "C" void casacore_cpp_aipsio_free_offsets(std::uint32_t* ptr) {
    std::free(ptr);
}

extern "C" void casacore_cpp_aipsio_free_error(char* ptr) {
    std::free(ptr);
}
