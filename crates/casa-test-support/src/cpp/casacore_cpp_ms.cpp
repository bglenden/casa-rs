// MeasurementSet interop fixtures and benchmarks for C++ casacore.
#include "casacore_cpp_common.h"

#include <casacore/casa/Arrays/Matrix.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/Tables/TableAttr.h>
#include <casacore/tables/Tables/TableRow.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iomanip>
#include <set>
#include <sstream>
#include <vector>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_ms;

namespace {

constexpr casacore::uInt kNumCorr = 4;
constexpr casacore::uInt kNumChan = 16;
constexpr double kBaseTimeSeconds = 59000.0 * 86400.0;

struct StableDigest {
    uint64_t a = 1469598103934665603ULL;
    uint64_t b = 1099511628211ULL;

    void write_bytes(const void* data, size_t size)
    {
        const auto* bytes = static_cast<const unsigned char*>(data);
        for (size_t i = 0; i < size; ++i) {
            a ^= static_cast<uint64_t>(bytes[i]);
            a *= 1099511628211ULL;
            b ^= static_cast<uint64_t>(bytes[i]);
            b *= 14029467366897019727ULL;
        }
    }

    void write_u8(uint8_t value) { write_bytes(&value, sizeof(value)); }
    void write_u32(uint32_t value) { write_bytes(&value, sizeof(value)); }
    void write_u64(uint64_t value) { write_bytes(&value, sizeof(value)); }
    void write_i64(int64_t value) { write_bytes(&value, sizeof(value)); }
    void write_bool(bool value) { write_u8(value ? 1 : 0); }

    void write_string(const std::string& value)
    {
        write_u64(static_cast<uint64_t>(value.size()));
        write_bytes(value.data(), value.size());
    }

    std::string hex() const
    {
        std::ostringstream os;
        os << std::hex << std::setfill('0')
           << std::setw(16) << a
           << std::setw(16) << b;
        return os.str();
    }
};

std::filesystem::path normalize_existing_path(const std::filesystem::path& path)
{
    std::error_code ec;
    auto normalized = std::filesystem::weakly_canonical(path, ec);
    if (ec) {
        return path.lexically_normal();
    }
    return normalized;
}

std::filesystem::path normalize_existing_path(const casacore::String& path)
{
    return normalize_existing_path(std::filesystem::path(path.c_str()));
}

std::string relative_label(const std::filesystem::path& root, const std::filesystem::path& path)
{
    std::error_code ec;
    auto rel = std::filesystem::relative(path, root, ec);
    if (!ec && !rel.empty() && rel.native()[0] != '.') {
        return rel.generic_string();
    }
    rel = path.lexically_relative(root);
    if (!rel.empty() && rel.native()[0] != '.') {
        return rel.generic_string();
    }
    return path.generic_string();
}

std::string data_type_tag(casacore::DataType type)
{
    switch (type) {
    case casacore::TpBool: return "Bool";
    case casacore::TpUChar: return "UInt8";
    case casacore::TpShort: return "Int16";
    case casacore::TpUShort: return "UInt16";
    case casacore::TpInt: return "Int32";
    case casacore::TpUInt: return "UInt32";
    case casacore::TpInt64: return "Int64";
    case casacore::TpFloat: return "Float32";
    case casacore::TpDouble: return "Float64";
    case casacore::TpComplex: return "Complex32";
    case casacore::TpDComplex: return "Complex64";
    case casacore::TpString: return "String";
    case casacore::TpRecord: return "Record";
    case casacore::TpTable: return "TableRef";
    case casacore::TpArrayBool: return "ArrayBool";
    case casacore::TpArrayUChar: return "ArrayUInt8";
    case casacore::TpArrayShort: return "ArrayInt16";
    case casacore::TpArrayUShort: return "ArrayUInt16";
    case casacore::TpArrayInt: return "ArrayInt32";
    case casacore::TpArrayUInt: return "ArrayUInt32";
    case casacore::TpArrayInt64: return "ArrayInt64";
    case casacore::TpArrayFloat: return "ArrayFloat32";
    case casacore::TpArrayDouble: return "ArrayFloat64";
    case casacore::TpArrayComplex: return "ArrayComplex32";
    case casacore::TpArrayDComplex: return "ArrayComplex64";
    case casacore::TpArrayString: return "ArrayString";
    default: return "Other";
    }
}

std::string array_shape_tag(const casacore::RecordInterface& rec, casacore::uInt i)
{
    std::ostringstream os;
    os << '[';
    auto emit_shape = [&](const auto& arr) {
        const auto& shape = arr.shape();
        for (casacore::uInt axis = 0; axis < shape.nelements(); ++axis) {
            if (axis > 0) {
                os << ',';
            }
            os << shape[axis];
        }
    };
    switch (rec.type(i)) {
    case casacore::TpArrayBool: emit_shape(rec.asArrayBool(i)); break;
    case casacore::TpArrayUChar: emit_shape(rec.asArrayuChar(i)); break;
    case casacore::TpArrayShort: emit_shape(rec.asArrayShort(i)); break;
    case casacore::TpArrayInt: emit_shape(rec.asArrayInt(i)); break;
    case casacore::TpArrayUInt: emit_shape(rec.asArrayuInt(i)); break;
    case casacore::TpArrayInt64: emit_shape(rec.asArrayInt64(i)); break;
    case casacore::TpArrayFloat: emit_shape(rec.asArrayFloat(i)); break;
    case casacore::TpArrayDouble: emit_shape(rec.asArrayDouble(i)); break;
    case casacore::TpArrayComplex: emit_shape(rec.asArrayComplex(i)); break;
    case casacore::TpArrayDComplex: emit_shape(rec.asArrayDComplex(i)); break;
    case casacore::TpArrayString: emit_shape(rec.asArrayString(i)); break;
    default: break;
    }
    os << ']';
    return os.str();
}

void digest_value(StableDigest& digest,
                  const casacore::RecordInterface& rec,
                  casacore::uInt i,
                  const std::filesystem::path& root_path,
                  std::set<std::filesystem::path>& discovered_refs,
                  const std::filesystem::path& owner_path);

template<typename T, typename Emit>
void digest_array_value(StableDigest& digest, const casacore::Array<T>& value, Emit emit)
{
    const auto& shape = value.shape();
    digest.write_u64(static_cast<uint64_t>(shape.nelements()));
    for (casacore::uInt axis = 0; axis < shape.nelements(); ++axis) {
        digest.write_u64(static_cast<uint64_t>(shape[axis]));
    }
    if (value.nelements() == 0) {
        return;
    }
    casacore::IPosition index(shape.nelements(), 0);
    while (true) {
        emit(value(index));
        casacore::uInt axis = 0;
        for (; axis < shape.nelements(); ++axis) {
            index[axis] += 1;
            if (index[axis] < shape[axis]) {
                break;
            }
            index[axis] = 0;
        }
        if (axis == shape.nelements()) {
            break;
        }
    }
}

void digest_record(StableDigest& digest,
                   const casacore::RecordInterface& rec,
                   const std::filesystem::path& root_path,
                   std::set<std::filesystem::path>& discovered_refs,
                   const std::filesystem::path& owner_path)
{
    digest.write_u64(static_cast<uint64_t>(rec.nfields()));
    std::vector<casacore::uInt> fields(rec.nfields());
    for (casacore::uInt i = 0; i < rec.nfields(); ++i) {
        fields[i] = i;
    }
    std::sort(fields.begin(), fields.end(), [&](casacore::uInt lhs, casacore::uInt rhs) {
        return rec.description().name(lhs) < rec.description().name(rhs);
    });
    for (casacore::uInt i : fields) {
        digest.write_string(rec.description().name(i));
        digest.write_string(data_type_tag(rec.type(i)));
        digest_value(digest, rec, i, root_path, discovered_refs, owner_path);
    }
}

void digest_value(StableDigest& digest,
                  const casacore::RecordInterface& rec,
                  casacore::uInt i,
                  const std::filesystem::path& root_path,
                  std::set<std::filesystem::path>& discovered_refs,
                  const std::filesystem::path& owner_path)
{
    switch (rec.type(i)) {
    case casacore::TpBool:
        digest.write_bool(rec.asBool(i));
        break;
    case casacore::TpUChar:
        digest.write_u8(rec.asuChar(i));
        break;
    case casacore::TpShort: {
        auto value = rec.asShort(i);
        digest.write_bytes(&value, sizeof(value));
        break;
    }
    case casacore::TpInt: {
        auto value = rec.asInt(i);
        digest.write_bytes(&value, sizeof(value));
        break;
    }
    case casacore::TpUInt: {
        auto value = rec.asuInt(i);
        digest.write_bytes(&value, sizeof(value));
        break;
    }
    case casacore::TpInt64:
        digest.write_i64(rec.asInt64(i));
        break;
    case casacore::TpFloat: {
        auto bits = rec.asFloat(i);
        digest.write_bytes(&bits, sizeof(bits));
        break;
    }
    case casacore::TpDouble: {
        auto bits = rec.asDouble(i);
        digest.write_bytes(&bits, sizeof(bits));
        break;
    }
    case casacore::TpComplex: {
        auto value = rec.asComplex(i);
        auto re = value.real();
        auto im = value.imag();
        digest.write_bytes(&re, sizeof(re));
        digest.write_bytes(&im, sizeof(im));
        break;
    }
    case casacore::TpDComplex: {
        auto value = rec.asDComplex(i);
        auto re = value.real();
        auto im = value.imag();
        digest.write_bytes(&re, sizeof(re));
        digest.write_bytes(&im, sizeof(im));
        break;
    }
    case casacore::TpString:
        digest.write_string(rec.asString(i));
        break;
    case casacore::TpRecord: {
        casacore::TableRecord nested = rec.asRecord(i);
        digest_record(digest, nested, root_path, discovered_refs, owner_path);
        break;
    }
    case casacore::TpTable: {
        const auto* table_record = dynamic_cast<const casacore::TableRecord*>(&rec);
        if (table_record == nullptr) {
            throw std::runtime_error("TpTable encountered outside TableRecord");
        }
        auto ref_name = std::filesystem::path(table_record->tableAttributes(i).name().c_str());
        auto ref_path = normalize_existing_path(owner_path / ref_name);
        discovered_refs.insert(ref_path);
        digest.write_string(relative_label(root_path, ref_path));
        break;
    }
    case casacore::TpArrayBool: {
        auto value = rec.asArrayBool(i);
        digest_array_value(digest, value, [&](bool v) { digest.write_bool(v); });
        break;
    }
    case casacore::TpArrayUChar: {
        auto value = rec.asArrayuChar(i);
        digest_array_value(digest, value, [&](casacore::uChar v) { digest.write_u8(v); });
        break;
    }
    case casacore::TpArrayShort: {
        auto value = rec.asArrayShort(i);
        digest_array_value(digest, value, [&](casacore::Short v) {
            digest.write_bytes(&v, sizeof(v));
        });
        break;
    }
    case casacore::TpArrayInt: {
        auto value = rec.asArrayInt(i);
        digest_array_value(digest, value, [&](casacore::Int v) {
            digest.write_bytes(&v, sizeof(v));
        });
        break;
    }
    case casacore::TpArrayUInt: {
        auto value = rec.asArrayuInt(i);
        digest_array_value(digest, value, [&](casacore::uInt v) {
            digest.write_bytes(&v, sizeof(v));
        });
        break;
    }
    case casacore::TpArrayInt64: {
        auto value = rec.asArrayInt64(i);
        digest_array_value(digest, value, [&](casacore::Int64 v) { digest.write_i64(v); });
        break;
    }
    case casacore::TpArrayFloat: {
        auto value = rec.asArrayFloat(i);
        digest_array_value(digest, value, [&](casacore::Float v) {
            digest.write_bytes(&v, sizeof(v));
        });
        break;
    }
    case casacore::TpArrayDouble: {
        auto value = rec.asArrayDouble(i);
        digest_array_value(digest, value, [&](casacore::Double v) {
            digest.write_bytes(&v, sizeof(v));
        });
        break;
    }
    case casacore::TpArrayComplex: {
        auto value = rec.asArrayComplex(i);
        digest_array_value(digest, value, [&](casacore::Complex v) {
            auto re = v.real();
            auto im = v.imag();
            digest.write_bytes(&re, sizeof(re));
            digest.write_bytes(&im, sizeof(im));
        });
        break;
    }
    case casacore::TpArrayDComplex: {
        auto value = rec.asArrayDComplex(i);
        digest_array_value(digest, value, [&](casacore::DComplex v) {
            auto re = v.real();
            auto im = v.imag();
            digest.write_bytes(&re, sizeof(re));
            digest.write_bytes(&im, sizeof(im));
        });
        break;
    }
    case casacore::TpArrayString: {
        auto value = rec.asArrayString(i);
        digest_array_value(digest, value, [&](const casacore::String& v) {
            digest.write_string(v);
        });
        break;
    }
    default:
        throw std::runtime_error("unsupported record field data type in digest");
    }
}

StableDigest digest_table_info(const casacore::Table& table)
{
    StableDigest digest;
    digest.write_string(table.tableInfo().type());
    digest.write_string(table.tableInfo().subType());
    return digest;
}

StableDigest digest_table_schema(const casacore::Table& table)
{
    StableDigest digest;
    auto td = table.tableDesc();
    digest.write_u64(static_cast<uint64_t>(td.ncolumn()));
    for (casacore::uInt i = 0; i < td.ncolumn(); ++i) {
        const auto& col = td[i];
        digest.write_string(col.name());
        digest.write_string(data_type_tag(col.trueDataType()));
        digest.write_u32(static_cast<uint32_t>(col.options()));
        digest.write_i64(static_cast<int64_t>(col.ndim()));
        const auto& shape = col.shape();
        digest.write_u64(static_cast<uint64_t>(shape.nelements()));
        for (casacore::uInt axis = 0; axis < shape.nelements(); ++axis) {
            digest.write_i64(static_cast<int64_t>(shape[axis]));
        }
    }
    return digest;
}

void append_table_schema_column_lines(std::vector<std::string>& manifest, const casacore::Table& table, const std::string& label)
{
    auto td = table.tableDesc();
    for (casacore::uInt i = 0; i < td.ncolumn(); ++i) {
        const auto& col = td[i];
        StableDigest digest;
        digest.write_string(col.name());
        digest.write_string(data_type_tag(col.trueDataType()));
        digest.write_u32(static_cast<uint32_t>(col.options()));
        digest.write_i64(static_cast<int64_t>(col.ndim()));
        const auto& shape = col.shape();
        digest.write_u64(static_cast<uint64_t>(shape.nelements()));
        for (casacore::uInt axis = 0; axis < shape.nelements(); ++axis) {
            digest.write_i64(static_cast<int64_t>(shape[axis]));
        }
        manifest.push_back(label + ":SCHEMACOL:" + std::string(col.name().c_str()) + " " + digest.hex());
    }
}

StableDigest digest_table_column_keywords(const casacore::Table& table,
                                          const std::filesystem::path& root_path,
                                          std::set<std::filesystem::path>& discovered_refs)
{
    StableDigest digest;
    const auto owner_path = normalize_existing_path(table.tableName());
    auto td = table.tableDesc();
    digest.write_u64(static_cast<uint64_t>(td.ncolumn()));
    for (casacore::uInt i = 0; i < td.ncolumn(); ++i) {
        const auto& col = td[i];
        digest.write_string(col.name());
        digest_record(digest, col.keywordSet(), root_path, discovered_refs, owner_path);
    }
    return digest;
}

StableDigest digest_table_keywords_digest(const casacore::Table& table,
                                          const std::filesystem::path& root_path,
                                          std::set<std::filesystem::path>& discovered_refs)
{
    StableDigest digest;
    const auto owner_path = normalize_existing_path(table.tableName());
    digest_record(digest, table.keywordSet(), root_path, discovered_refs, owner_path);
    return digest;
}

StableDigest digest_table_rows(const casacore::Table& table,
                               const std::filesystem::path& root_path,
                               std::set<std::filesystem::path>& discovered_refs)
{
    StableDigest digest;
    digest.write_u64(static_cast<uint64_t>(table.nrow()));
    auto desc = table.tableDesc();
    std::vector<casacore::uInt> columns(desc.ncolumn());
    for (casacore::uInt i = 0; i < desc.ncolumn(); ++i) {
        columns[i] = i;
    }
    std::sort(columns.begin(), columns.end(), [&](casacore::uInt lhs, casacore::uInt rhs) {
        return desc[lhs].name() < desc[rhs].name();
    });
    const auto owner_path = normalize_existing_path(table.tableName());
    casacore::ROTableRow row(table, casacore::False);
    for (casacore::rownr_t rownr = 0; rownr < table.nrow(); ++rownr) {
        const auto& record = row.get(rownr, casacore::True);
        digest.write_u64(static_cast<uint64_t>(columns.size()));
        for (casacore::uInt col_index : columns) {
            const auto& col = desc[col_index];
            const std::string name = col.name().c_str();
            const bool defined = casacore::TableColumn(table, col.name()).isDefined(rownr);
            digest.write_string(name);
            digest.write_string(data_type_tag(col.trueDataType()));
            digest.write_bool(defined);
            if (defined) {
                const int field_index = record.description().fieldNumber(col.name());
                if (field_index < 0) {
                    throw std::runtime_error("defined row field missing from TableRow record: " + name);
                }
                digest_value(
                    digest,
                    record,
                    static_cast<casacore::uInt>(field_index),
                    root_path,
                    discovered_refs,
                    owner_path);
            } else {
                digest.write_i64(std::max(0, col.ndim()));
            }
        }
    }

    return digest;
}

StableDigest digest_table_row(const casacore::Table& table,
                              casacore::rownr_t rownr,
                              const std::filesystem::path& root_path,
                              std::set<std::filesystem::path>& discovered_refs)
{
    StableDigest digest;
    auto desc = table.tableDesc();
    std::vector<casacore::uInt> columns(desc.ncolumn());
    for (casacore::uInt i = 0; i < desc.ncolumn(); ++i) {
        columns[i] = i;
    }
    std::sort(columns.begin(), columns.end(), [&](casacore::uInt lhs, casacore::uInt rhs) {
        return desc[lhs].name() < desc[rhs].name();
    });
    const auto owner_path = normalize_existing_path(table.tableName());
    casacore::ROTableRow row(table, casacore::False);
    const auto& record = row.get(rownr, casacore::True);
    digest.write_u64(static_cast<uint64_t>(columns.size()));
    for (casacore::uInt col_index : columns) {
        const auto& col = desc[col_index];
        const std::string name = col.name().c_str();
        const bool defined = casacore::TableColumn(table, col.name()).isDefined(rownr);
        digest.write_string(name);
        digest.write_string(data_type_tag(col.trueDataType()));
        digest.write_bool(defined);
        if (defined) {
            const int field_index = record.description().fieldNumber(col.name());
            if (field_index < 0) {
                throw std::runtime_error("defined row field missing from TableRow record: " + name);
            }
            digest_value(
                digest,
                record,
                static_cast<casacore::uInt>(field_index),
                root_path,
                discovered_refs,
                owner_path);
        } else {
            digest.write_i64(std::max(0, col.ndim()));
        }
    }
    return digest;
}

void append_manifest_line(std::vector<std::string>& manifest,
                          const std::string& label,
                          const StableDigest& digest)
{
    manifest.push_back(label + " " + digest.hex());
}

void digest_table_recursive(const casacore::Table& table,
                            const std::string& label,
                            const std::filesystem::path& root_path,
                            std::set<std::filesystem::path>& visited,
                            std::vector<std::string>& manifest)
{
    const auto table_path = normalize_existing_path(table.tableName());
    if (!visited.insert(table_path).second) {
        return;
    }

    std::set<std::filesystem::path> discovered_refs;
    append_manifest_line(manifest, label + ":INFO", digest_table_info(table));
    append_manifest_line(manifest, label + ":SCHEMA", digest_table_schema(table));
    append_table_schema_column_lines(manifest, table, label);
    append_manifest_line(
        manifest,
        label + ":COLKW",
        digest_table_column_keywords(table, root_path, discovered_refs));
    append_manifest_line(
        manifest,
        label + ":TABLEKW",
        digest_table_keywords_digest(table, root_path, discovered_refs));
    append_manifest_line(manifest, label + ":ROWS", digest_table_rows(table, root_path, discovered_refs));

    std::vector<std::filesystem::path> refs(discovered_refs.begin(), discovered_refs.end());
    std::sort(refs.begin(), refs.end());
    for (const auto& ref_path : refs) {
        if (visited.find(ref_path) != visited.end()) {
            continue;
        }
        if (!std::filesystem::exists(ref_path)) {
            manifest.push_back(
                "EXTRA_MISSING:" + relative_label(root_path, ref_path) + " MISSING");
            continue;
        }
        casacore::Table ref_table(ref_path.generic_string(), casacore::Table::Old);
        digest_table_recursive(
            ref_table,
            "EXTRA:" + relative_label(root_path, ref_path),
            root_path,
            visited,
            manifest);
    }
}

std::string digest_manifest_impl(const std::string& path)
{
    const auto root_path = normalize_existing_path(std::filesystem::path(path));
    casacore::MeasurementSet ms(path, casacore::Table::Old);

    std::set<std::filesystem::path> visited;
    std::vector<std::string> manifest;

    digest_table_recursive(ms, "MAIN", root_path, visited, manifest);

    auto digest_if_present = [&](const auto& table, const char* label) {
        if (!table.isNull()) {
            digest_table_recursive(table, label, root_path, visited, manifest);
        }
    };

    digest_if_present(ms.antenna(), "ANTENNA");
    digest_if_present(ms.dataDescription(), "DATA_DESCRIPTION");
    digest_if_present(ms.feed(), "FEED");
    digest_if_present(ms.field(), "FIELD");
    digest_if_present(ms.flagCmd(), "FLAG_CMD");
    digest_if_present(ms.history(), "HISTORY");
    digest_if_present(ms.observation(), "OBSERVATION");
    digest_if_present(ms.pointing(), "POINTING");
    digest_if_present(ms.polarization(), "POLARIZATION");
    digest_if_present(ms.processor(), "PROCESSOR");
    digest_if_present(ms.spectralWindow(), "SPECTRAL_WINDOW");
    digest_if_present(ms.state(), "STATE");
    digest_if_present(ms.doppler(), "DOPPLER");
    digest_if_present(ms.freqOffset(), "FREQ_OFFSET");
    digest_if_present(ms.source(), "SOURCE");
    digest_if_present(ms.sysCal(), "SYSCAL");
    digest_if_present(ms.weather(), "WEATHER");

    StableDigest global;
    for (const auto& line : manifest) {
        global.write_string(line);
    }

    std::ostringstream os;
    for (const auto& line : manifest) {
        os << line << '\n';
    }
    os << "GLOBAL " << global.hex() << '\n';
    return os.str();
}

const casacore::Table& ms_table_by_label(const casacore::MeasurementSet& ms, const std::string& label)
{
    if (label == "MAIN") return ms;
    if (label == "ANTENNA") return ms.antenna();
    if (label == "DATA_DESCRIPTION") return ms.dataDescription();
    if (label == "FEED") return ms.feed();
    if (label == "FIELD") return ms.field();
    if (label == "FLAG_CMD") return ms.flagCmd();
    if (label == "HISTORY") return ms.history();
    if (label == "OBSERVATION") return ms.observation();
    if (label == "POINTING") return ms.pointing();
    if (label == "POLARIZATION") return ms.polarization();
    if (label == "PROCESSOR") return ms.processor();
    if (label == "SPECTRAL_WINDOW") return ms.spectralWindow();
    if (label == "STATE") return ms.state();
    if (label == "DOPPLER") return ms.doppler();
    if (label == "FREQ_OFFSET") return ms.freqOffset();
    if (label == "SOURCE") return ms.source();
    if (label == "SYSCAL") return ms.sysCal();
    if (label == "WEATHER") return ms.weather();
    throw std::runtime_error("unknown MeasurementSet table label: " + label);
}

std::string table_row_digest_impl(const std::string& path, const std::string& table_label, uint64_t row)
{
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    const auto& table = ms_table_by_label(ms, table_label);
    if (row >= table.nrow()) {
        throw std::runtime_error(
            "row " + std::to_string(row) + " out of range for " + table_label +
            " (" + std::to_string(table.nrow()) + " rows)");
    }
    const auto root_path = normalize_existing_path(std::filesystem::path(path));
    std::set<std::filesystem::path> discovered_refs;
    StableDigest digest = digest_table_row(
        table,
        static_cast<casacore::rownr_t>(row),
        root_path,
        discovered_refs);
    return digest.hex();
}

std::string table_row_field_manifest_impl(
    const std::string& path,
    const std::string& table_label,
    uint64_t row)
{
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    const auto& table = ms_table_by_label(ms, table_label);
    if (row >= table.nrow()) {
        throw std::runtime_error(
            "row " + std::to_string(row) + " out of range for " + table_label +
            " (" + std::to_string(table.nrow()) + " rows)");
    }
    const auto root_path = normalize_existing_path(std::filesystem::path(path));
    const auto owner_path = normalize_existing_path(table.tableName());
    std::set<std::filesystem::path> discovered_refs;
    casacore::ROTableRow row_reader(table, casacore::False);
    const auto& record = row_reader.get(row, casacore::True);
    auto desc = table.tableDesc();
    std::vector<casacore::uInt> columns(desc.ncolumn());
    for (casacore::uInt i = 0; i < desc.ncolumn(); ++i) {
        columns[i] = i;
    }
    std::sort(columns.begin(), columns.end(), [&](casacore::uInt lhs, casacore::uInt rhs) {
        return desc[lhs].name() < desc[rhs].name();
    });

    std::ostringstream os;
    for (casacore::uInt col_index : columns) {
        const auto& col = desc[col_index];
        const auto name = col.name();
        const bool defined = casacore::TableColumn(table, name).isDefined(row);
        StableDigest digest;
        digest.write_string(name);
        digest.write_string(data_type_tag(col.trueDataType()));
        digest.write_bool(defined);
        os << name << '\t' << data_type_tag(col.trueDataType());
        if (defined) {
            const int field_index = record.description().fieldNumber(name);
            if (field_index < 0) {
                throw std::runtime_error("defined row field missing from TableRow record: " + std::string(name.c_str()));
            }
            digest_value(
                digest,
                record,
                static_cast<casacore::uInt>(field_index),
                root_path,
                discovered_refs,
                owner_path);
            if (data_type_tag(col.trueDataType()).rfind("Array", 0) == 0) {
                os << '\t' << array_shape_tag(record, static_cast<casacore::uInt>(field_index));
            }
        } else {
            const auto ndim = std::max(0, col.ndim());
            digest.write_i64(ndim);
            os << '\t' << "undefined(ndim=" << ndim << ')';
        }
        os << '\t' << digest.hex() << '\n';
    }
    return os.str();
}

void bench_main_rows_impl(const std::string& path,
                          uint64_t* out_read_ns,
                          std::string* out_digest)
{
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    std::set<std::filesystem::path> discovered_refs;
    const auto root_path = normalize_existing_path(ms.tableName());
    auto t0 = std::chrono::steady_clock::now();
    auto digest = digest_table_rows(ms, root_path, discovered_refs);
    auto t1 = std::chrono::steady_clock::now();
    *out_read_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    *out_digest = digest.hex();
}

void bench_open_main_rows_impl(const std::string& path,
                               uint64_t* out_open_and_read_ns,
                               std::string* out_digest)
{
    auto t0 = std::chrono::steady_clock::now();
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    std::set<std::filesystem::path> discovered_refs;
    const auto root_path = normalize_existing_path(ms.tableName());
    auto digest = digest_table_rows(ms, root_path, discovered_refs);
    auto t1 = std::chrono::steady_clock::now();
    *out_open_and_read_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    *out_digest = digest.hex();
}

casacore::Array<casacore::Complex> make_data_array(size_t row)
{
    casacore::IPosition shape(2, kNumCorr, kNumChan);
    casacore::Array<casacore::Complex> data(shape);
    const float offset = static_cast<float>(row * kNumCorr * kNumChan);
    size_t i = 0;
    for (casacore::uInt chan = 0; chan < kNumChan; ++chan) {
        for (casacore::uInt corr = 0; corr < kNumCorr; ++corr) {
            const float value = offset + static_cast<float>(i);
            data(casacore::IPosition(2, corr, chan)) =
                casacore::Complex(value, -0.5f * value);
            ++i;
        }
    }
    return data;
}

void populate_fixture_subtables(casacore::MeasurementSet& ms)
{
    {
        casacore::MSAntenna antenna = ms.antenna();
        antenna.addRow(2);

        casacore::ScalarColumn<casacore::String> name(antenna, "NAME");
        casacore::ScalarColumn<casacore::String> station(antenna, "STATION");
        casacore::ScalarColumn<casacore::String> type(antenna, "TYPE");
        casacore::ScalarColumn<casacore::String> mount(antenna, "MOUNT");
        casacore::ArrayColumn<casacore::Double> position(antenna, "POSITION");
        casacore::ArrayColumn<casacore::Double> offset(antenna, "OFFSET");
        casacore::ScalarColumn<casacore::Double> dish(antenna, "DISH_DIAMETER");
        casacore::ScalarColumn<casacore::Bool> flagRow(antenna, "FLAG_ROW");

        casacore::Vector<casacore::Double> pos0(3);
        pos0(0) = 0.0;
        pos0(1) = 10.0;
        pos0(2) = 20.0;
        casacore::Vector<casacore::Double> pos1(3);
        pos1(0) = 100.0;
        pos1(1) = 110.0;
        pos1(2) = 120.0;
        casacore::Vector<casacore::Double> zeroOffset(3);
        zeroOffset = 0.0;

        name.put(0, "ANT0");
        station.put(0, "STA0");
        type.put(0, "GROUND-BASED");
        mount.put(0, "ALT-AZ");
        position.put(0, pos0);
        offset.put(0, zeroOffset);
        dish.put(0, 12.0);
        flagRow.put(0, false);

        name.put(1, "ANT1");
        station.put(1, "STA1");
        type.put(1, "GROUND-BASED");
        mount.put(1, "ALT-AZ");
        position.put(1, pos1);
        offset.put(1, zeroOffset);
        dish.put(1, 13.0);
        flagRow.put(1, false);
    }

    {
        casacore::MSField field = ms.field();
        field.addRow(1);

        casacore::ScalarColumn<casacore::String> name(field, "NAME");
        casacore::ScalarColumn<casacore::String> code(field, "CODE");
        casacore::ScalarColumn<casacore::Int> numPoly(field, "NUM_POLY");
        casacore::ArrayColumn<casacore::Double> delayDir(field, "DELAY_DIR");
        casacore::ArrayColumn<casacore::Double> phaseDir(field, "PHASE_DIR");
        casacore::ArrayColumn<casacore::Double> refDir(field, "REFERENCE_DIR");
        casacore::ScalarColumn<casacore::Int> sourceId(field, "SOURCE_ID");
        casacore::ScalarColumn<casacore::Double> time(field, "TIME");
        casacore::ScalarColumn<casacore::Bool> flagRow(field, "FLAG_ROW");

        casacore::Matrix<casacore::Double> direction(2, 1);
        direction(0, 0) = 1.0;
        direction(1, 0) = 0.5;

        name.put(0, "TEST_FIELD");
        code.put(0, "T");
        numPoly.put(0, 0);
        delayDir.put(0, direction);
        phaseDir.put(0, direction);
        refDir.put(0, direction);
        sourceId.put(0, -1);
        time.put(0, kBaseTimeSeconds);
        flagRow.put(0, false);
    }

    {
        casacore::MSPolarization pol = ms.polarization();
        pol.addRow(1);

        casacore::ScalarColumn<casacore::Int> numCorr(pol, "NUM_CORR");
        casacore::ArrayColumn<casacore::Int> corrType(pol, "CORR_TYPE");
        casacore::ArrayColumn<casacore::Int> corrProduct(pol, "CORR_PRODUCT");
        casacore::ScalarColumn<casacore::Bool> flagRow(pol, "FLAG_ROW");

        casacore::Vector<casacore::Int> corrTypeVals(kNumCorr);
        corrTypeVals(0) = 5;
        corrTypeVals(1) = 6;
        corrTypeVals(2) = 7;
        corrTypeVals(3) = 8;

        casacore::Matrix<casacore::Int> corrProductVals(2, kNumCorr);
        corrProductVals(0, 0) = 0;
        corrProductVals(1, 0) = 0;
        corrProductVals(0, 1) = 0;
        corrProductVals(1, 1) = 1;
        corrProductVals(0, 2) = 1;
        corrProductVals(1, 2) = 0;
        corrProductVals(0, 3) = 1;
        corrProductVals(1, 3) = 1;

        numCorr.put(0, static_cast<casacore::Int>(kNumCorr));
        corrType.put(0, corrTypeVals);
        corrProduct.put(0, corrProductVals);
        flagRow.put(0, false);
    }

    {
        casacore::MSSpectralWindow spw = ms.spectralWindow();
        spw.addRow(1);

        casacore::ScalarColumn<casacore::Int> numChan(spw, "NUM_CHAN");
        casacore::ScalarColumn<casacore::String> name(spw, "NAME");
        casacore::ScalarColumn<casacore::Double> refFrequency(spw, "REF_FREQUENCY");
        casacore::ScalarColumn<casacore::Double> totalBandwidth(spw, "TOTAL_BANDWIDTH");
        casacore::ArrayColumn<casacore::Double> chanFreq(spw, "CHAN_FREQ");
        casacore::ArrayColumn<casacore::Double> chanWidth(spw, "CHAN_WIDTH");
        casacore::ArrayColumn<casacore::Double> effectiveBw(spw, "EFFECTIVE_BW");
        casacore::ArrayColumn<casacore::Double> resolution(spw, "RESOLUTION");
        casacore::ScalarColumn<casacore::Int> measFreqRef(spw, "MEAS_FREQ_REF");
        casacore::ScalarColumn<casacore::Int> netSideband(spw, "NET_SIDEBAND");
        casacore::ScalarColumn<casacore::Int> freqGroup(spw, "FREQ_GROUP");
        casacore::ScalarColumn<casacore::String> freqGroupName(spw, "FREQ_GROUP_NAME");
        casacore::ScalarColumn<casacore::Int> ifConvChain(spw, "IF_CONV_CHAIN");
        casacore::ScalarColumn<casacore::Bool> flagRow(spw, "FLAG_ROW");

        casacore::Vector<casacore::Double> freqs(kNumChan);
        casacore::Vector<casacore::Double> widths(kNumChan);
        for (casacore::uInt chan = 0; chan < kNumChan; ++chan) {
            freqs(chan) = 1.0e9 + chan * 1.0e6;
            widths(chan) = 1.0e6;
        }

        numChan.put(0, static_cast<casacore::Int>(kNumChan));
        name.put(0, "SPW0");
        refFrequency.put(0, 1.0e9);
        totalBandwidth.put(0, static_cast<casacore::Double>(kNumChan) * 1.0e6);
        chanFreq.put(0, freqs);
        chanWidth.put(0, widths);
        effectiveBw.put(0, widths);
        resolution.put(0, widths);
        measFreqRef.put(0, 5);
        netSideband.put(0, 1);
        freqGroup.put(0, 0);
        freqGroupName.put(0, "");
        ifConvChain.put(0, 0);
        flagRow.put(0, false);
    }

    {
        casacore::MSDataDescription dd = ms.dataDescription();
        dd.addRow(1);

        casacore::ScalarColumn<casacore::Int> spectralWindowId(dd, "SPECTRAL_WINDOW_ID");
        casacore::ScalarColumn<casacore::Int> polarizationId(dd, "POLARIZATION_ID");
        casacore::ScalarColumn<casacore::Bool> flagRow(dd, "FLAG_ROW");

        spectralWindowId.put(0, 0);
        polarizationId.put(0, 0);
        flagRow.put(0, false);
    }
}

void populate_fixture_rows(casacore::MeasurementSet& ms, size_t nrows)
{
    ms.addRow(nrows);

    casacore::ScalarColumn<casacore::Int> antenna1(ms, "ANTENNA1");
    casacore::ScalarColumn<casacore::Int> antenna2(ms, "ANTENNA2");
    casacore::ScalarColumn<casacore::Int> arrayId(ms, "ARRAY_ID");
    casacore::ScalarColumn<casacore::Int> dataDescId(ms, "DATA_DESC_ID");
    casacore::ScalarColumn<casacore::Double> exposure(ms, "EXPOSURE");
    casacore::ScalarColumn<casacore::Int> feed1(ms, "FEED1");
    casacore::ScalarColumn<casacore::Int> feed2(ms, "FEED2");
    casacore::ScalarColumn<casacore::Int> fieldId(ms, "FIELD_ID");
    casacore::ArrayColumn<casacore::Bool> flag(ms, "FLAG");
    casacore::ArrayColumn<casacore::Bool> flagCategory(ms, "FLAG_CATEGORY");
    casacore::ScalarColumn<casacore::Bool> flagRow(ms, "FLAG_ROW");
    casacore::ScalarColumn<casacore::Double> interval(ms, "INTERVAL");
    casacore::ScalarColumn<casacore::Int> observationId(ms, "OBSERVATION_ID");
    casacore::ScalarColumn<casacore::Int> processorId(ms, "PROCESSOR_ID");
    casacore::ScalarColumn<casacore::Int> scanNumber(ms, "SCAN_NUMBER");
    casacore::ArrayColumn<casacore::Float> sigma(ms, "SIGMA");
    casacore::ScalarColumn<casacore::Int> stateId(ms, "STATE_ID");
    casacore::ScalarColumn<casacore::Double> time(ms, "TIME");
    casacore::ScalarColumn<casacore::Double> timeCentroid(ms, "TIME_CENTROID");
    casacore::ArrayColumn<casacore::Double> uvw(ms, "UVW");
    casacore::ArrayColumn<casacore::Float> weight(ms, "WEIGHT");
    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");

    casacore::Matrix<casacore::Bool> flagVals(kNumCorr, kNumChan);
    flagVals = false;
    casacore::Array<casacore::Bool> flagCategoryVals(casacore::IPosition(3, 1, kNumCorr, kNumChan));
    flagCategoryVals = false;
    casacore::Vector<casacore::Float> sigmaVals(kNumCorr);
    sigmaVals = 1.0f;
    casacore::Vector<casacore::Float> weightVals(kNumCorr);
    weightVals = 1.0f;
    casacore::Vector<casacore::Double> uvwVals(3);
    uvwVals = 0.0;

    for (size_t row = 0; row < nrows; ++row) {
        antenna1.put(row, 0);
        antenna2.put(row, 1);
        arrayId.put(row, 0);
        dataDescId.put(row, 0);
        exposure.put(row, 10.0);
        feed1.put(row, 0);
        feed2.put(row, 0);
        fieldId.put(row, 0);
        flag.put(row, flagVals);
        flagCategory.put(row, flagCategoryVals);
        flagRow.put(row, false);
        interval.put(row, 10.0);
        observationId.put(row, 0);
        processorId.put(row, 0);
        scanNumber.put(row, static_cast<casacore::Int>(row + 1));
        sigma.put(row, sigmaVals);
        stateId.put(row, 0);
        time.put(row, kBaseTimeSeconds + static_cast<double>(row));
        timeCentroid.put(row, kBaseTimeSeconds + static_cast<double>(row));
        uvw.put(row, uvwVals);
        weight.put(row, weightVals);
        data.put(row, make_data_array(row));
    }
}

void write_fixture_impl(const std::string& path, size_t nrows)
{
    casacore::TableDesc td(casacore::MS::requiredTableDesc());
    casacore::MS::addColumnToDesc(td, casacore::MS::DATA, 2);

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::MeasurementSet ms(setup, 0);
    ms.createDefaultSubtables(casacore::Table::New);
    populate_fixture_subtables(ms);
    populate_fixture_rows(ms, nrows);
    ms.flush(true);
}

void verify_complex_sample(const casacore::Array<casacore::Complex>& data,
                           casacore::uInt corr,
                           casacore::uInt chan,
                           float expectedReal,
                           float expectedImag)
{
    const casacore::Complex sample = data(casacore::IPosition(2, corr, chan));
    if (std::fabs(sample.real() - expectedReal) > 1e-5f ||
        std::fabs(sample.imag() - expectedImag) > 1e-5f) {
        throw std::runtime_error(
            "DATA sample mismatch at [" + std::to_string(corr) + "," +
            std::to_string(chan) + "]: expected (" + std::to_string(expectedReal) +
            "," + std::to_string(expectedImag) + "), got (" +
            std::to_string(sample.real()) + "," + std::to_string(sample.imag()) + ")");
    }
}

void verify_fixture_impl(const std::string& path)
{
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    if (!ms.validate()) {
        throw std::runtime_error("MeasurementSet::validate() reported false");
    }
    if (ms.tableInfo().type() != "Measurement Set") {
        throw std::runtime_error("unexpected table.info type: " +
                                 std::string(ms.tableInfo().type().c_str()));
    }
    if (!ms.keywordSet().isDefined("ANTENNA") ||
        !ms.keywordSet().isDefined("DATA_DESCRIPTION") ||
        !ms.keywordSet().isDefined("FIELD") ||
        !ms.keywordSet().isDefined("POLARIZATION") ||
        !ms.keywordSet().isDefined("SPECTRAL_WINDOW")) {
        throw std::runtime_error("missing required MS subtable keyword");
    }
    if (!ms.keywordSet().isDefined("MS_VERSION")) {
        throw std::runtime_error("MS_VERSION keyword missing");
    }
    if (ms.nrow() != 6) {
        throw std::runtime_error("expected 6 main rows, got " + std::to_string(ms.nrow()));
    }
    if (ms.antenna().nrow() != 2) {
        throw std::runtime_error("expected 2 antenna rows, got " +
                                 std::to_string(ms.antenna().nrow()));
    }
    if (ms.field().nrow() != 1) {
        throw std::runtime_error("expected 1 field row, got " +
                                 std::to_string(ms.field().nrow()));
    }
    if (ms.polarization().nrow() != 1) {
        throw std::runtime_error("expected 1 polarization row, got " +
                                 std::to_string(ms.polarization().nrow()));
    }
    if (ms.spectralWindow().nrow() != 1) {
        throw std::runtime_error("expected 1 spectral-window row, got " +
                                 std::to_string(ms.spectralWindow().nrow()));
    }
    if (ms.dataDescription().nrow() != 1) {
        throw std::runtime_error("expected 1 data-description row, got " +
                                 std::to_string(ms.dataDescription().nrow()));
    }

    casacore::ScalarColumn<casacore::String> antennaName(ms.antenna(), "NAME");
    if (antennaName(0) != "ANT0" || antennaName(1) != "ANT1") {
        throw std::runtime_error("ANTENNA names do not match expected fixture");
    }

    casacore::ScalarColumn<casacore::String> fieldName(ms.field(), "NAME");
    if (fieldName(0) != "TEST_FIELD") {
        throw std::runtime_error("FIELD name mismatch");
    }

    casacore::ScalarColumn<casacore::Int> numCorr(ms.polarization(), "NUM_CORR");
    if (numCorr(0) != static_cast<casacore::Int>(kNumCorr)) {
        throw std::runtime_error("NUM_CORR mismatch");
    }

    casacore::ScalarColumn<casacore::Int> numChan(ms.spectralWindow(), "NUM_CHAN");
    casacore::ScalarColumn<casacore::String> spwName(ms.spectralWindow(), "NAME");
    if (numChan(0) != static_cast<casacore::Int>(kNumChan) || spwName(0) != "SPW0") {
        throw std::runtime_error("SPECTRAL_WINDOW contents mismatch");
    }

    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");
    casacore::Array<casacore::Complex> row0 = data(0);
    casacore::Array<casacore::Complex> row5 = data(5);
    if (row0.shape() != casacore::IPosition(2, kNumCorr, kNumChan)) {
        throw std::runtime_error("row 0 DATA shape mismatch");
    }
    if (row5.shape() != casacore::IPosition(2, kNumCorr, kNumChan)) {
        throw std::runtime_error("row 5 DATA shape mismatch");
    }
    verify_complex_sample(row0, 0, 0, 0.0f, -0.0f);
    verify_complex_sample(row0, 3, 15, 63.0f, -31.5f);
    verify_complex_sample(row5, 0, 0, 320.0f, -160.0f);
    verify_complex_sample(row5, 3, 15, 383.0f, -191.5f);
}

void bench_create_open_impl(const std::string& path,
                            uint64_t nrows,
                            uint64_t* out_create_ns,
                            uint64_t* out_open_ns,
                            uint64_t* out_read_ns)
{
    auto t0 = std::chrono::steady_clock::now();
    write_fixture_impl(path, static_cast<size_t>(nrows));
    auto t1 = std::chrono::steady_clock::now();
    *out_create_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    t0 = std::chrono::steady_clock::now();
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    t1 = std::chrono::steady_clock::now();
    *out_open_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");
    volatile float sink = 0.0f;
    t0 = std::chrono::steady_clock::now();
    for (uint64_t row = 0; row < nrows; ++row) {
        casacore::Array<casacore::Complex> sample = data(row);
        sink += sample(casacore::IPosition(2, 0, 0)).real();
    }
    t1 = std::chrono::steady_clock::now();
    *out_read_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    if (sink < 0.0f) {
        throw std::runtime_error("unexpected negative sink in benchmark");
    }
}

}  // namespace

extern "C" {

int32_t cpp_ms_write_basic_fixture(const char* path, char** out_error)
{
    try {
        write_fixture_impl(path, 6);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_write_basic_fixture");
        return -1;
    }
}

int32_t cpp_ms_verify_basic_fixture(const char* path, char** out_error)
{
    try {
        verify_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_verify_basic_fixture");
        return -1;
    }
}

int32_t cpp_ms_bench_create_open(
    const char* path,
    uint64_t nrows,
    uint64_t* out_create_ns,
    uint64_t* out_open_ns,
    uint64_t* out_read_ns,
    char** out_error)
{
    try {
        bench_create_open_impl(path, nrows, out_create_ns, out_open_ns, out_read_ns);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_bench_create_open");
        return -1;
    }
}

int32_t cpp_ms_digest_manifest(
    const char* path,
    char** out_manifest,
    char** out_error)
{
    try {
        auto manifest = digest_manifest_impl(path);
        *out_manifest = make_error(manifest);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_digest_manifest");
        return -1;
    }
}

int32_t cpp_ms_table_row_digest(
    const char* path,
    const char* table_label,
    uint64_t row,
    char** out_digest,
    char** out_error)
{
    try {
        auto digest = table_row_digest_impl(path, table_label, row);
        *out_digest = make_error(digest);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_table_row_digest");
        return -1;
    }
}

int32_t cpp_ms_table_row_field_manifest(
    const char* path,
    const char* table_label,
    uint64_t row,
    char** out_manifest,
    char** out_error)
{
    try {
        auto manifest = table_row_field_manifest_impl(path, table_label, row);
        *out_manifest = make_error(manifest);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_table_row_field_manifest");
        return -1;
    }
}

int32_t cpp_ms_bench_main_rows(
    const char* path,
    uint64_t* out_read_ns,
    char** out_digest,
    char** out_error)
{
    try {
        std::string digest;
        bench_main_rows_impl(path, out_read_ns, &digest);
        *out_digest = make_error(digest);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_bench_main_rows");
        return -1;
    }
}

int32_t cpp_ms_bench_open_main_rows(
    const char* path,
    uint64_t* out_open_and_read_ns,
    char** out_digest,
    char** out_error)
{
    try {
        std::string digest;
        bench_open_main_rows_impl(path, out_open_and_read_ns, &digest);
        *out_digest = make_error(digest);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_bench_open_main_rows");
        return -1;
    }
}

}  // extern "C"
