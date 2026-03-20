// SPDX-License-Identifier: LGPL-3.0-or-later
// Typed PagedImage interop shim for cross-language verification.
#include "casacore_cpp_common.h"
#include <chrono>
#include <casacore/images/Images/TempImage.h>
#include <casacore/images/Images/PagedImage.h>
#include <casacore/images/Images/ImageInfo.h>
#include <casacore/images/Images/ImageExprParse.h>
#include <casacore/images/Images/ImageExpr.h>
#include <casacore/images/Images/ImageOpener.h>
#include <casacore/coordinates/Coordinates/CoordinateUtil.h>
#include <casacore/lattices/Lattices.h>
#include <casacore/lattices/LEL/LatticeExpr.h>
#include <casacore/lattices/Lattices/TiledShape.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/casa/Containers/Block.h>
#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/tables/DataMan/TiledStManAccessor.h>

using casacore_shim::make_error;

namespace {

void copy_bool_array_to_output(
    casacore::Array<casacore::Bool>& arr,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out
);

casacore::CoordinateSystem make_default_coords(int32_t ndim) {
    casacore::CoordinateSystem cs;
    casacore::CoordinateUtil::addDirAxes(cs);
    if (ndim > 2) {
        casacore::CoordinateUtil::addFreqAxis(cs);
    }
    if (ndim > 3) {
        casacore::CoordinateUtil::addStokesAxis(cs, 1);
    }
    return cs;
}

casacore::IPosition make_shape(const int32_t* shape_ptr, int32_t ndim) {
    casacore::IPosition shape(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        shape[i] = shape_ptr[i];
    }
    return shape;
}

template <typename Pixel>
void create_real_pagedimage_impl(
    const std::string& path,
    const int32_t* shape_ptr,
    int32_t ndim,
    const Pixel* data_ptr,
    int64_t ndata,
    const char* units
) {
    const auto shape = make_shape(shape_ptr, ndim);
    const auto cs = make_default_coords(ndim);
    casacore::PagedImage<Pixel> img(casacore::TiledShape(shape), cs, path);

    if (units && units[0] != '\0') {
        img.setUnits(casacore::Unit(units));
    }

    if (data_ptr && ndata > 0) {
        casacore::Array<Pixel> arr(shape);
        const int64_t n = std::min(ndata, static_cast<int64_t>(arr.nelements()));
        casacore::Bool deleteIt;
        Pixel* storage = arr.getStorage(deleteIt);
        std::memcpy(storage, data_ptr, n * sizeof(Pixel));
        arr.putStorage(storage, deleteIt);
        img.put(arr);
    }

    img.flush();
}

template <typename Pixel>
void create_real_pagedimage_tiled_impl(
    const std::string& path,
    const int32_t* shape_ptr,
    const int32_t* tile_ptr,
    int32_t ndim,
    const Pixel* data_ptr,
    int64_t ndata,
    const char* units
) {
    const auto shape = make_shape(shape_ptr, ndim);
    casacore::IPosition tile(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        tile[i] = tile_ptr[i];
    }
    const auto cs = make_default_coords(ndim);
    casacore::PagedImage<Pixel> img(casacore::TiledShape(shape, tile), cs, path);

    if (units && units[0] != '\0') {
        img.setUnits(casacore::Unit(units));
    }

    if (data_ptr && ndata > 0) {
        casacore::Array<Pixel> arr(shape);
        const int64_t n = std::min(ndata, static_cast<int64_t>(arr.nelements()));
        casacore::Bool deleteIt;
        Pixel* storage = arr.getStorage(deleteIt);
        std::memcpy(storage, data_ptr, n * sizeof(Pixel));
        arr.putStorage(storage, deleteIt);
        img.put(arr);
    }

    img.flush();
}

template <typename Pixel>
void read_real_pagedimage_impl(
    const std::string& path,
    Pixel* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<Pixel> img(path);
    casacore::Array<Pixel> arr = img.get();
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const Pixel* storage = arr.getStorage(deleteIt);
    std::memcpy(data_out, storage, n * sizeof(Pixel));
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

void create_complex32_pagedimage_impl(
    const std::string& path,
    const int32_t* shape_ptr,
    int32_t ndim,
    const float* data_ptr,
    int64_t ncomplex,
    const char* units
) {
    const auto shape = make_shape(shape_ptr, ndim);
    const auto cs = make_default_coords(ndim);
    casacore::PagedImage<casacore::Complex> img(casacore::TiledShape(shape), cs, path);

    if (units && units[0] != '\0') {
        img.setUnits(casacore::Unit(units));
    }

    if (data_ptr && ncomplex > 0) {
        casacore::Array<casacore::Complex> arr(shape);
        const int64_t n = std::min(ncomplex, static_cast<int64_t>(arr.nelements()));
        casacore::Bool deleteIt;
        casacore::Complex* storage = arr.getStorage(deleteIt);
        for (int64_t i = 0; i < n; ++i) {
            storage[i] = casacore::Complex(data_ptr[2 * i], data_ptr[2 * i + 1]);
        }
        arr.putStorage(storage, deleteIt);
        img.put(arr);
    }

    img.flush();
}

void read_complex32_pagedimage_impl(
    const std::string& path,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Complex> img(path);
    casacore::Array<casacore::Complex> arr = img.get();
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const casacore::Complex* storage = arr.getStorage(deleteIt);
    for (int64_t i = 0; i < n; ++i) {
        data_out[2 * i] = storage[i].real();
        data_out[2 * i + 1] = storage[i].imag();
    }
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

void create_complex64_pagedimage_impl(
    const std::string& path,
    const int32_t* shape_ptr,
    int32_t ndim,
    const double* data_ptr,
    int64_t ncomplex,
    const char* units
) {
    const auto shape = make_shape(shape_ptr, ndim);
    const auto cs = make_default_coords(ndim);
    casacore::PagedImage<casacore::DComplex> img(casacore::TiledShape(shape), cs, path);

    if (units && units[0] != '\0') {
        img.setUnits(casacore::Unit(units));
    }

    if (data_ptr && ncomplex > 0) {
        casacore::Array<casacore::DComplex> arr(shape);
        const int64_t n = std::min(ncomplex, static_cast<int64_t>(arr.nelements()));
        casacore::Bool deleteIt;
        casacore::DComplex* storage = arr.getStorage(deleteIt);
        for (int64_t i = 0; i < n; ++i) {
            storage[i] = casacore::DComplex(data_ptr[2 * i], data_ptr[2 * i + 1]);
        }
        arr.putStorage(storage, deleteIt);
        img.put(arr);
    }

    img.flush();
}

void read_complex64_pagedimage_impl(
    const std::string& path,
    double* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::DComplex> img(path);
    casacore::Array<casacore::DComplex> arr = img.get();
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const casacore::DComplex* storage = arr.getStorage(deleteIt);
    for (int64_t i = 0; i < n; ++i) {
        data_out[2 * i] = storage[i].real();
        data_out[2 * i + 1] = storage[i].imag();
    }
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

void read_pagedimage_shape_impl(
    const std::string& path,
    int32_t* shape_out,
    int32_t max_ndim,
    int32_t* ndim_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    casacore::IPosition shape = img.shape();
    const int32_t nd = std::min(static_cast<int32_t>(shape.size()), max_ndim);
    for (int32_t i = 0; i < nd; ++i) {
        shape_out[i] = static_cast<int32_t>(shape[i]);
    }
    *ndim_out = nd;
}

void read_pagedimage_units_impl(const std::string& path, char* buf, int32_t bufsize) {
    casacore::PagedImage<casacore::Float> img(path);
    const std::string units = img.units().getName();
    const int32_t n = std::min(static_cast<int32_t>(units.size()), bufsize - 1);
    std::memcpy(buf, units.c_str(), n);
    buf[n] = '\0';
}

void create_tempimage_float_materialized_impl(
    const std::string& path,
    const int32_t* shape_ptr,
    int32_t ndim,
    const float* data_ptr,
    int64_t ndata,
    const char* units,
    const char* object_name,
    const char* image_type
) {
    const auto shape = make_shape(shape_ptr, ndim);
    const auto coords = make_default_coords(ndim);
    casacore::TempImage<casacore::Float> temp(casacore::TiledShape(shape), coords, 0);

    if (units && units[0] != '\0') {
        temp.setUnits(casacore::Unit(units));
    }

    if ((object_name && object_name[0] != '\0') || (image_type && image_type[0] != '\0')) {
        casacore::ImageInfo info;
        if (object_name && object_name[0] != '\0') {
            info.setObjectName(casacore::String(object_name));
        }
        if (image_type && image_type[0] != '\0') {
            info.setImageType(casacore::ImageInfo::imageType(casacore::String(image_type)));
        }
        temp.setImageInfo(info);
    }

    if (data_ptr && ndata > 0) {
        casacore::Array<casacore::Float> arr(shape);
        const int64_t n = std::min(ndata, static_cast<int64_t>(arr.nelements()));
        casacore::Bool deleteIt;
        casacore::Float* storage = arr.getStorage(deleteIt);
        std::memcpy(storage, data_ptr, n * sizeof(casacore::Float));
        arr.putStorage(storage, deleteIt);
        temp.put(arr);
    }

    temp.makeMask("flags", true, true, true, true);
    temp.tempClose();
    temp.reopen();

    casacore::PagedImage<casacore::Float> out(casacore::TiledShape(shape), coords, path);
    out.copyData(temp);
    out.setUnits(temp.units());
    out.setImageInfo(temp.imageInfo());
    if (temp.hasPixelMask()) {
        out.makeMask("flags", true, true, true, false);
        out.pixelMask().copyData(temp.pixelMask());
        out.setDefaultMask(temp.getDefaultMask());
    }
    out.flush();
}

void read_pagedimage_coordinate_count_impl(const std::string& path, int32_t* count_out) {
    casacore::PagedImage<casacore::Float> img(path);
    *count_out = static_cast<int32_t>(img.coordinates().nCoordinates());
}

void read_pagedimage_default_mask_name_impl(
    const std::string& path,
    char* buf,
    int32_t bufsize
) {
    casacore::PagedImage<casacore::Float> img(path);
    const std::string name = img.getDefaultMask();
    const int32_t n = std::min(static_cast<int32_t>(name.size()), bufsize - 1);
    std::memcpy(buf, name.c_str(), n);
    buf[n] = '\0';
}

void read_pagedimage_default_mask_impl(
    const std::string& path,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    if (!img.hasPixelMask()) {
        *nread_out = 0;
        return;
    }
    casacore::Array<casacore::Bool> arr = img.pixelMask().get();
    copy_bool_array_to_output(arr, data_out, max_size, nread_out);
}

void read_pagedimage_imageinfo_object_name_impl(
    const std::string& path,
    char* buf,
    int32_t bufsize
) {
    casacore::PagedImage<casacore::Float> img(path);
    const std::string object_name = img.imageInfo().objectName();
    const int32_t n = std::min(static_cast<int32_t>(object_name.size()), bufsize - 1);
    std::memcpy(buf, object_name.c_str(), n);
    buf[n] = '\0';
}

void read_pagedimage_imageinfo_type_impl(
    const std::string& path,
    char* buf,
    int32_t bufsize
) {
    casacore::PagedImage<casacore::Float> img(path);
    const std::string image_type = casacore::ImageInfo::imageType(img.imageInfo().imageType());
    const int32_t n = std::min(static_cast<int32_t>(image_type.size()), bufsize - 1);
    std::memcpy(buf, image_type.c_str(), n);
    buf[n] = '\0';
}

void read_pagedimage_slice_impl(
    const std::string& path,
    const int32_t* start_ptr,
    const int32_t* length_ptr,
    int32_t ndim,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    casacore::IPosition start(ndim), length(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        start[i] = start_ptr[i];
        length[i] = length_ptr[i];
    }
    casacore::Slicer slicer(start, length);
    casacore::Array<casacore::Float> arr;
    img.doGetSlice(arr, slicer);
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const casacore::Float* storage = arr.getStorage(deleteIt);
    std::memcpy(data_out, storage, n * sizeof(float));
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

template <typename Pixel>
void copy_numeric_array_to_output(
    casacore::Array<Pixel>& arr,
    Pixel* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const Pixel* storage = arr.getStorage(deleteIt);
    std::memcpy(data_out, storage, n * sizeof(Pixel));
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

void copy_bool_array_to_output(
    casacore::Array<casacore::Bool>& arr,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    const int64_t n = std::min(max_size, static_cast<int64_t>(arr.nelements()));
    casacore::Bool deleteIt;
    const casacore::Bool* storage = arr.getStorage(deleteIt);
    for (int64_t i = 0; i < n; ++i) {
        data_out[i] = storage[i] ? 1 : 0;
    }
    arr.freeStorage(storage, deleteIt);
    *nread_out = n;
}

casacore::LatticeExprNode apply_float_unary_expr(
    const casacore::PagedImage<casacore::Float>& img,
    int32_t op
) {
    switch (op) {
    case 0:  return -img;
    case 1:  return exp(img);
    case 2:  return sin(img);
    case 3:  return cos(img);
    case 4:  return tan(img);
    case 5:  return asin(img);
    case 6:  return acos(img);
    case 7:  return atan(img);
    case 8:  return sinh(img);
    case 9:  return cosh(img);
    case 10: return tanh(img);
    case 11: return log(img);
    case 12: return log10(img);
    case 13: return sqrt(img);
    case 14: return abs(img);
    case 15: return ceil(img);
    case 16: return floor(img);
    case 17: return round(img);
    case 18: return sign(img);
    case 19: return conj(img);
    default:
        throw std::runtime_error("unsupported float unary image expression op");
    }
}

casacore::LatticeExprNode apply_float_binary_expr(
    const casacore::PagedImage<casacore::Float>& lhs,
    const casacore::PagedImage<casacore::Float>& rhs,
    int32_t op
) {
    switch (op) {
    case 0: return lhs + rhs;
    case 1: return lhs * rhs;
    case 2: return lhs - rhs;
    case 3: return lhs / rhs;
    case 4: return pow(lhs, rhs);
    case 5: return fmod(lhs, rhs);
    case 6: return atan2(lhs, rhs);
    case 7: return min(lhs, rhs);
    case 8: return max(lhs, rhs);
    default:
        throw std::runtime_error("unsupported float binary image expression op");
    }
}

casacore::LatticeExprNode apply_float_scalar_expr(
    const casacore::PagedImage<casacore::Float>& img,
    casacore::Float scalar,
    int32_t op
) {
    switch (op) {
    case 0: return img + scalar;
    case 1: return img * scalar;
    case 2: return img - scalar;
    case 3: return img / scalar;
    case 4: return pow(img, scalar);
    case 5: return fmod(img, scalar);
    case 6: return atan2(img, scalar);
    case 7: return min(img, scalar);
    case 8: return max(img, scalar);
    default:
        throw std::runtime_error("unsupported float scalar image expression op");
    }
}

casacore::LatticeExprNode apply_float_compare_expr(
    const casacore::PagedImage<casacore::Float>& img,
    int32_t op,
    casacore::Float scalar
) {
    switch (op) {
    case 0: return img > scalar;
    case 1: return img < scalar;
    case 2: return img >= scalar;
    case 3: return img <= scalar;
    case 4: return img == scalar;
    case 5: return img != scalar;
    default:
        throw std::runtime_error("unsupported float compare image expression op");
    }
}

casacore::LatticeExprNode apply_bool_logical_expr(
    const casacore::LatticeExprNode& lhs,
    int32_t op,
    const casacore::LatticeExprNode& rhs
) {
    switch (op) {
    case 0:
        return lhs && rhs;
    case 1:
        return lhs || rhs;
    default:
        throw std::runtime_error("unsupported logical image expression op");
    }
}

void eval_pagedimage_float_unary_expr_impl(
    const std::string& path,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    casacore::LatticeExpr<casacore::Float> expr(apply_float_unary_expr(img, op));
    casacore::Array<casacore::Float> arr = expr.get();
    copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
}

void eval_pagedimage_float_binary_expr_impl(
    const std::string& lhs_path,
    const std::string& rhs_path,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> lhs(lhs_path);
    casacore::PagedImage<casacore::Float> rhs(rhs_path);
    casacore::LatticeExpr<casacore::Float> expr(apply_float_binary_expr(lhs, rhs, op));
    casacore::Array<casacore::Float> arr = expr.get();
    copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
}

void eval_pagedimage_float_scalar_expr_impl(
    const std::string& path,
    casacore::Float scalar,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    casacore::LatticeExpr<casacore::Float> expr(apply_float_scalar_expr(img, scalar, op));
    casacore::Array<casacore::Float> arr = expr.get();
    copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
}

void eval_pagedimage_float_range_mask_expr_impl(
    const std::string& path,
    int32_t lower_cmp,
    casacore::Float lower,
    int32_t logical_op,
    int32_t upper_cmp,
    casacore::Float upper,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    const casacore::LatticeExprNode lhs = apply_float_compare_expr(img, lower_cmp, lower);
    const casacore::LatticeExprNode rhs = apply_float_compare_expr(img, upper_cmp, upper);
    casacore::LatticeExpr<casacore::Bool> expr(apply_bool_logical_expr(lhs, logical_op, rhs));
    casacore::Array<casacore::Bool> arr = expr.get();
    copy_bool_array_to_output(arr, data_out, max_size, nread_out);
}

void eval_pagedimage_float_closeout_expr_slice_impl(
    const std::string& path,
    const int32_t* start_ptr,
    const int32_t* length_ptr,
    int32_t ndim,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out
) {
    casacore::PagedImage<casacore::Float> img(path);
    casacore::IPosition start(ndim), length(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        start[i] = start_ptr[i];
        length[i] = length_ptr[i];
    }
    casacore::Slicer slicer(start, length);

    const casacore::LatticeExprNode lhs = sqrt(img + 1.0f);
    const casacore::LatticeExprNode rhs = fmod(pow(img + 0.5f, 2.0f), 3.0f) + 0.25f;
    casacore::LatticeExpr<casacore::Float> expr(max(atan2(lhs, rhs), 0.5f));

    casacore::Array<casacore::Float> arr;
    expr.doGetSlice(arr, slicer);
    copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
}

// Bypass PagedImage::setMaximumCacheSize (which has a units bug: it converts
// pixels→bytes then passes to TiledStMan which interprets the value as MiB).
// Instead, use ROTiledStManAccessor directly with the correct MiB value.
template<typename T>
void setImageCacheMiB(casacore::PagedImage<T>& img, int32_t cacheMiB) {
    if (cacheMiB <= 0) return;
    casacore::ROTiledStManAccessor accessor(img.table(), "map", true);
    accessor.setMaximumCacheSize(static_cast<casacore::uInt>(cacheMiB));
}

} // namespace

extern "C" {

int32_t cpp_create_pagedimage_float(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const float* data,
    int64_t ndata,
    const char* units,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_real_pagedimage_impl<casacore::Float>(path, shape, ndim, data, ndata, units);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in create_pagedimage_float");
        return -1;
    }
}

int32_t cpp_create_pagedimage_float_tiled(
    const char* path,
    const int32_t* shape,
    const int32_t* tile,
    int32_t ndim,
    const float* data,
    int64_t ndata,
    const char* units,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_real_pagedimage_tiled_impl<casacore::Float>(
            path, shape, tile, ndim, data, ndata, units
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception creating tiled PagedImage<Float>");
        return -1;
    }
}

int32_t cpp_create_tempimage_float_materialized(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const float* data,
    int64_t ndata,
    const char* units,
    const char* object_name,
    const char* image_type,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_tempimage_float_materialized_impl(
            path, shape, ndim, data, ndata, units, object_name, image_type
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in create_tempimage_float_materialized");
        return -1;
    }
}

int32_t cpp_read_pagedimage_float(
    const char* path,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_real_pagedimage_impl<casacore::Float>(path, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_float");
        return -1;
    }
}

int32_t cpp_create_pagedimage_double(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const double* data,
    int64_t ndata,
    const char* units,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_real_pagedimage_impl<casacore::Double>(path, shape, ndim, data, ndata, units);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in create_pagedimage_double");
        return -1;
    }
}

int32_t cpp_read_pagedimage_double(
    const char* path,
    double* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_real_pagedimage_impl<casacore::Double>(path, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_double");
        return -1;
    }
}

int32_t cpp_create_pagedimage_complex32(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const float* data,
    int64_t ncomplex,
    const char* units,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_complex32_pagedimage_impl(path, shape, ndim, data, ncomplex, units);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in create_pagedimage_complex32");
        return -1;
    }
}

int32_t cpp_read_pagedimage_complex32(
    const char* path,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_complex32_pagedimage_impl(path, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_complex32");
        return -1;
    }
}

int32_t cpp_create_pagedimage_complex64(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const double* data,
    int64_t ncomplex,
    const char* units,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        create_complex64_pagedimage_impl(path, shape, ndim, data, ncomplex, units);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in create_pagedimage_complex64");
        return -1;
    }
}

int32_t cpp_read_pagedimage_complex64(
    const char* path,
    double* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_complex64_pagedimage_impl(path, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_complex64");
        return -1;
    }
}

int32_t cpp_read_pagedimage_shape(
    const char* path,
    int32_t* shape_out,
    int32_t max_ndim,
    int32_t* ndim_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_shape_impl(path, shape_out, max_ndim, ndim_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_shape");
        return -1;
    }
}

int32_t cpp_read_pagedimage_units(
    const char* path,
    char* buf,
    int32_t bufsize,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_units_impl(path, buf, bufsize);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_units");
        return -1;
    }
}

int32_t cpp_read_pagedimage_coordinate_count(
    const char* path,
    int32_t* count_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_coordinate_count_impl(path, count_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_coordinate_count");
        return -1;
    }
}

int32_t cpp_read_pagedimage_default_mask_name(
    const char* path,
    char* buf,
    int32_t bufsize,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_default_mask_name_impl(path, buf, bufsize);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_default_mask_name");
        return -1;
    }
}

int32_t cpp_read_pagedimage_default_mask(
    const char* path,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_default_mask_impl(path, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_default_mask");
        return -1;
    }
}

int32_t cpp_read_pagedimage_imageinfo_object_name(
    const char* path,
    char* buf,
    int32_t bufsize,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_imageinfo_object_name_impl(path, buf, bufsize);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_imageinfo_object_name");
        return -1;
    }
}

int32_t cpp_read_pagedimage_imageinfo_type(
    const char* path,
    char* buf,
    int32_t bufsize,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_imageinfo_type_impl(path, buf, bufsize);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_imageinfo_type");
        return -1;
    }
}

int32_t cpp_read_pagedimage_slice(
    const char* path,
    const int32_t* start,
    const int32_t* length,
    int32_t ndim,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        read_pagedimage_slice_impl(path, start, length, ndim, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in read_pagedimage_slice");
        return -1;
    }
}

int32_t cpp_eval_pagedimage_float_unary_expr(
    const char* path,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        eval_pagedimage_float_unary_expr_impl(path, op, data_out, max_size, nread_out);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_pagedimage_float_unary_expr");
        return -1;
    }
}

int32_t cpp_eval_pagedimage_float_binary_expr(
    const char* lhs_path,
    const char* rhs_path,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        eval_pagedimage_float_binary_expr_impl(
            lhs_path,
            rhs_path,
            op,
            data_out,
            max_size,
            nread_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_pagedimage_float_binary_expr");
        return -1;
    }
}

int32_t cpp_eval_pagedimage_float_scalar_expr(
    const char* path,
    float scalar,
    int32_t op,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        eval_pagedimage_float_scalar_expr_impl(
            path,
            scalar,
            op,
            data_out,
            max_size,
            nread_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_pagedimage_float_scalar_expr");
        return -1;
    }
}

int32_t cpp_eval_pagedimage_float_range_mask_expr(
    const char* path,
    int32_t lower_cmp,
    float lower,
    int32_t logical_op,
    int32_t upper_cmp,
    float upper,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        eval_pagedimage_float_range_mask_expr_impl(
            path,
            lower_cmp,
            lower,
            logical_op,
            upper_cmp,
            upper,
            data_out,
            max_size,
            nread_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_pagedimage_float_range_mask_expr");
        return -1;
    }
}

int32_t cpp_eval_pagedimage_float_closeout_expr_slice(
    const char* path,
    const int32_t* start,
    const int32_t* length,
    int32_t ndim,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        eval_pagedimage_float_closeout_expr_slice_impl(
            path,
            start,
            length,
            ndim,
            data_out,
            max_size,
            nread_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_pagedimage_float_closeout_expr_slice");
        return -1;
    }
}

int32_t cpp_eval_lel_expr_float(
    const char* expr,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    int32_t* shape_out,
    int32_t max_ndim,
    int32_t* ndim_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        casacore::Block<casacore::LatticeExprNode> temps;
        casacore::PtrBlock<const casacore::ImageRegion*> tempRegs;
        casacore::LatticeExprNode node = casacore::ImageExprParse::command(
            expr, temps, tempRegs);
        if (node.isScalar()) {
            if (max_size > 0) {
                switch (node.dataType()) {
                case casacore::TpFloat:
                    data_out[0] = node.getFloat();
                    break;
                case casacore::TpDouble:
                    data_out[0] = static_cast<float>(node.getDouble());
                    break;
                default:
                    throw (casacore::AipsError(
                        "cpp_eval_lel_expr_float - unsupported scalar data type"));
                }
                *nread_out = 1;
            } else {
                *nread_out = 0;
            }
            *ndim_out = 0;
            return 0;
        }
        casacore::LatticeExpr<casacore::Float> lattExpr(node);
        casacore::Array<casacore::Float> arr = lattExpr.get();
        copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
        casacore::IPosition shape = lattExpr.shape();
        const int32_t nd = std::min(static_cast<int32_t>(shape.size()), max_ndim);
        for (int32_t i = 0; i < nd; ++i) {
            shape_out[i] = static_cast<int32_t>(shape[i]);
        }
        *ndim_out = nd;
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_lel_expr_float");
        return -1;
    }
}

int32_t cpp_profile_lel_scalar_expr_float(
    const char* expr,
    int32_t passes,
    double* timings_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        auto eval_scalar = [](casacore::LatticeExprNode& node) -> float {
            switch (node.dataType()) {
            case casacore::TpFloat:
                return node.getFloat();
            case casacore::TpDouble:
                return static_cast<float>(node.getDouble());
            default:
                throw(casacore::AipsError(
                    "cpp_profile_lel_scalar_expr_float - unsupported scalar data type"));
            }
        };

        volatile float sink = 0.0f;

        auto t0 = std::chrono::steady_clock::now();
        for (int32_t i = 0; i < passes; ++i) {
            casacore::Block<casacore::LatticeExprNode> temps;
            casacore::PtrBlock<const casacore::ImageRegion*> tempRegs;
            casacore::LatticeExprNode node = casacore::ImageExprParse::command(
                expr, temps, tempRegs);
            if (!node.isScalar()) {
                throw(casacore::AipsError(
                    "cpp_profile_lel_scalar_expr_float - expression is not scalar"));
            }
            sink += eval_scalar(node);
        }
        auto parse_each_total = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0
        ).count();

        t0 = std::chrono::steady_clock::now();
        casacore::Block<casacore::LatticeExprNode> temps;
        casacore::PtrBlock<const casacore::ImageRegion*> tempRegs;
        casacore::LatticeExprNode node = casacore::ImageExprParse::command(
            expr, temps, tempRegs);
        if (!node.isScalar()) {
            throw(casacore::AipsError(
                "cpp_profile_lel_scalar_expr_float - expression is not scalar"));
        }
        for (int32_t i = 0; i < passes; ++i) {
            sink += eval_scalar(node);
        }
        auto parse_once_total = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0
        ).count();

        t0 = std::chrono::steady_clock::now();
        for (int32_t i = 0; i < passes; ++i) {
            sink += eval_scalar(node);
        }
        auto eval_only = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0
        ).count();

        (void)sink;
        timings_out[0] = parse_each_total;
        timings_out[1] = parse_once_total;
        timings_out[2] = eval_only;
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_profile_lel_scalar_expr_float");
        return -1;
    }
}

int32_t cpp_eval_lel_expr_bool(
    const char* expr,
    uint8_t* data_out,
    int64_t max_size,
    int64_t* nread_out,
    int32_t* shape_out,
    int32_t max_ndim,
    int32_t* ndim_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        casacore::Block<casacore::LatticeExprNode> temps;
        casacore::PtrBlock<const casacore::ImageRegion*> tempRegs;
        casacore::LatticeExprNode node = casacore::ImageExprParse::command(
            expr, temps, tempRegs);
        if (node.isScalar()) {
            if (max_size > 0) {
                data_out[0] = node.getBool() ? 1 : 0;
                *nread_out = 1;
            } else {
                *nread_out = 0;
            }
            *ndim_out = 0;
            return 0;
        }
        casacore::LatticeExpr<casacore::Bool> lattExpr(node);
        casacore::Array<casacore::Bool> arr = lattExpr.get();
        copy_bool_array_to_output(arr, data_out, max_size, nread_out);
        casacore::IPosition shape = lattExpr.shape();
        const int32_t nd = std::min(static_cast<int32_t>(shape.size()), max_ndim);
        for (int32_t i = 0; i < nd; ++i) {
            shape_out[i] = static_cast<int32_t>(shape[i]);
        }
        *ndim_out = nd;
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in eval_lel_expr_bool");
        return -1;
    }
}

// ---------- Expression file save (parse expr, then save as .imgexpr) ----------

int32_t cpp_save_lel_expr(
    const char* expr,
    const char* save_path,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        casacore::Block<casacore::LatticeExprNode> temps;
        casacore::PtrBlock<const casacore::ImageRegion*> tempRegs;
        casacore::LatticeExprNode node = casacore::ImageExprParse::command(
            expr, temps, tempRegs);
        auto lattExpr = casacore::LatticeExpr<casacore::Float>(node);
        auto exprStr = casacore::String(expr);
        casacore::ImageExpr<casacore::Float> img(lattExpr, exprStr);
        img.save(casacore::String(save_path));
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in save_lel_expr");
        return -1;
    }
}

// ---------- Expression file open (open .imgexpr, read pixels) ----------

int32_t cpp_open_lel_expr_float(
    const char* path,
    float* data_out,
    int64_t max_size,
    int64_t* nread_out,
    int32_t* shape_out,
    int32_t max_ndim,
    int32_t* ndim_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        casacore::LatticeBase* latt = casacore::ImageOpener::openImageExpr(
            casacore::String(path));
        if (!latt) {
            *out_error = make_error("ImageOpener::openImageExpr returned null");
            return -1;
        }
        // C++ LEL promotes floating-point literals (e.g. 2.0) to Double,
        // so an expression like 'img' * 2.0 yields ImageExpr<Double>.
        // Try Float first, then fall back to Double with conversion.
        casacore::IPosition shape;
        casacore::ImageExpr<casacore::Float>* fimg =
            dynamic_cast<casacore::ImageExpr<casacore::Float>*>(latt);
        if (fimg) {
            casacore::Array<casacore::Float> arr = fimg->get();
            copy_numeric_array_to_output(arr, data_out, max_size, nread_out);
            shape = fimg->shape();
        } else {
            casacore::ImageExpr<casacore::Double>* dimg =
                dynamic_cast<casacore::ImageExpr<casacore::Double>*>(latt);
            if (!dimg) {
                std::string msg = "opened image is not ImageExpr<Float> or <Double>, dataType=";
                msg += std::to_string(latt->dataType());
                delete latt;
                *out_error = make_error(msg.c_str());
                return -1;
            }
            casacore::Array<casacore::Double> darr = dimg->get();
            // Convert Double to Float for output.
            casacore::Array<casacore::Float> farr(darr.shape());
            casacore::convertArray(farr, darr);
            copy_numeric_array_to_output(farr, data_out, max_size, nread_out);
            shape = dimg->shape();
        }
        const int32_t nd = std::min(static_cast<int32_t>(shape.size()), max_ndim);
        for (int32_t i = 0; i < nd; ++i) {
            shape_out[i] = static_cast<int32_t>(shape[i]);
        }
        *ndim_out = nd;
        delete latt;
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in open_lel_expr_float");
        return -1;
    }
}

// ---------- Benchmark: plane-by-plane I/O with optional cache limit ----------
// timings_out: [create_ms, write_ms, read_ms]

int32_t cpp_bench_plane_by_plane(
    const char* path,
    const int32_t* shape_ptr,
    const int32_t* tile_ptr,
    int32_t ndim,
    int32_t max_cache_mib,
    double* timings_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        using Clock = std::chrono::high_resolution_clock;
        const auto shape = make_shape(shape_ptr, ndim);
        casacore::IPosition tile(ndim);
        for (int32_t i = 0; i < ndim; ++i) tile[i] = tile_ptr[i];

        const int64_t plane_size = shape[0] * shape[1];
        const int64_t nz = shape[2];

        // Create with explicit tile shape.
        auto t0 = Clock::now();
        {
            casacore::CoordinateSystem cs = make_default_coords(ndim);
            casacore::PagedImage<casacore::Float> img(
                casacore::TiledShape(shape, tile), cs, std::string(path));
            setImageCacheMiB(img, max_cache_mib);
            double create_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
            timings_out[0] = create_ms;

            // Write plane by plane.
            auto tw0 = Clock::now();
            casacore::Array<casacore::Float> plane(casacore::IPosition(3, shape[0], shape[1], 1));
            for (int64_t z = 0; z < nz; ++z) {
                casacore::Bool deleteIt;
                casacore::Float* storage = plane.getStorage(deleteIt);
                for (int64_t y = 0; y < shape[1]; ++y) {
                    for (int64_t x = 0; x < shape[0]; ++x) {
                        storage[x + y * shape[0]] =
                            static_cast<float>(x + y * shape[0] + z * plane_size);
                    }
                }
                plane.putStorage(storage, deleteIt);
                casacore::IPosition start(3, 0, 0, z);
                img.doPutSlice(plane, start, casacore::IPosition(3, 1, 1, 1));
            }
            img.flush();
            timings_out[1] = std::chrono::duration<double, std::milli>(Clock::now() - tw0).count();
        }

        // Reopen and read plane by plane.
        auto tr0 = Clock::now();
        {
            std::string img_path(path);
            casacore::PagedImage<casacore::Float> img(img_path);
            setImageCacheMiB(img, max_cache_mib);
            for (int64_t z = 0; z < nz; ++z) {
                casacore::IPosition start(3, 0, 0, z);
                casacore::IPosition length(3, shape[0], shape[1], 1);
                casacore::Slicer slicer(start, length);
                casacore::Array<casacore::Float> slice;
                img.doGetSlice(slice, slicer);
            }
        }
        timings_out[2] = std::chrono::duration<double, std::milli>(Clock::now() - tr0).count();

        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in bench_plane_by_plane");
        return -1;
    }
}

// Benchmark spectrum-by-spectrum (1,1,nz) I/O with optional cache limit.
// timings_out: [create_ms, write_ms, read_ms]
int32_t cpp_bench_spectrum_by_spectrum(
    const char* path,
    const int32_t* shape_ptr,
    const int32_t* tile_ptr,
    int32_t ndim,
    int32_t max_cache_mib,
    double* timings_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        using Clock = std::chrono::high_resolution_clock;
        const auto shape = make_shape(shape_ptr, ndim);
        casacore::IPosition tile(ndim);
        for (int32_t i = 0; i < ndim; ++i) tile[i] = tile_ptr[i];

        const int64_t nx = shape[0];
        const int64_t ny = shape[1];
        const int64_t nz = shape[2];
        const int64_t plane_size = nx * ny;

        // Create with explicit tile shape.
        auto t0 = Clock::now();
        {
            casacore::CoordinateSystem cs = make_default_coords(ndim);
            casacore::PagedImage<casacore::Float> img(
                casacore::TiledShape(shape, tile), cs, std::string(path));
            setImageCacheMiB(img, max_cache_mib);
            double create_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
            timings_out[0] = create_ms;

            // Write spectrum by spectrum.
            auto tw0 = Clock::now();
            casacore::Array<casacore::Float> spectrum(casacore::IPosition(3, 1, 1, nz));
            for (int64_t y = 0; y < ny; ++y) {
                for (int64_t x = 0; x < nx; ++x) {
                    casacore::Bool deleteIt;
                    casacore::Float* storage = spectrum.getStorage(deleteIt);
                    for (int64_t z = 0; z < nz; ++z) {
                        storage[z] =
                            static_cast<float>(x + y * nx + z * plane_size);
                    }
                    spectrum.putStorage(storage, deleteIt);
                    casacore::IPosition start(3, x, y, 0);
                    img.doPutSlice(spectrum, start, casacore::IPosition(3, 1, 1, 1));
                }
            }
            img.flush();
            timings_out[1] = std::chrono::duration<double, std::milli>(Clock::now() - tw0).count();
        }

        // Reopen and read spectrum by spectrum.
        auto tr0 = Clock::now();
        {
            std::string img_path(path);
            casacore::PagedImage<casacore::Float> img(img_path);
            setImageCacheMiB(img, max_cache_mib);
            for (int64_t y = 0; y < ny; ++y) {
                for (int64_t x = 0; x < nx; ++x) {
                    casacore::IPosition start(3, x, y, 0);
                    casacore::IPosition length(3, 1, 1, nz);
                    casacore::Slicer slicer(start, length);
                    casacore::Array<casacore::Float> slice;
                    img.doGetSlice(slice, slicer);
                }
            }
        }
        timings_out[2] = std::chrono::duration<double, std::milli>(Clock::now() - tr0).count();

        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in bench_spectrum_by_spectrum");
        return -1;
    }
}

int32_t cpp_bench_plane_by_plane_complex(
    const char* path,
    const int32_t* shape_ptr,
    const int32_t* tile_ptr,
    int32_t ndim,
    int32_t max_cache_mib,
    double* timings_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        using Clock = std::chrono::high_resolution_clock;
        const auto shape = make_shape(shape_ptr, ndim);
        casacore::IPosition tile(ndim);
        for (int32_t i = 0; i < ndim; ++i) tile[i] = tile_ptr[i];

        const int64_t plane_size = shape[0] * shape[1];
        const int64_t nz = shape[2];

        // Create with explicit tile shape.
        auto t0 = Clock::now();
        {
            casacore::CoordinateSystem cs = make_default_coords(ndim);
            casacore::PagedImage<casacore::Complex> img(
                casacore::TiledShape(shape, tile), cs, std::string(path));
            setImageCacheMiB(img, max_cache_mib);
            double create_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
            timings_out[0] = create_ms;

            // Write plane by plane.
            auto tw0 = Clock::now();
            casacore::Array<casacore::Complex> plane(casacore::IPosition(3, shape[0], shape[1], 1));
            for (int64_t z = 0; z < nz; ++z) {
                casacore::Bool deleteIt;
                casacore::Complex* storage = plane.getStorage(deleteIt);
                for (int64_t y = 0; y < shape[1]; ++y) {
                    for (int64_t x = 0; x < shape[0]; ++x) {
                        float val = static_cast<float>(x + y * shape[0] + z * plane_size);
                        storage[x + y * shape[0]] = casacore::Complex(val, -val);
                    }
                }
                plane.putStorage(storage, deleteIt);
                casacore::IPosition start(3, 0, 0, z);
                img.doPutSlice(plane, start, casacore::IPosition(3, 1, 1, 1));
            }
            img.flush();
            timings_out[1] = std::chrono::duration<double, std::milli>(Clock::now() - tw0).count();
        }

        // Reopen and read plane by plane.
        auto tr0 = Clock::now();
        {
            std::string img_path(path);
            casacore::PagedImage<casacore::Complex> img(img_path);
            setImageCacheMiB(img, max_cache_mib);
            for (int64_t z = 0; z < nz; ++z) {
                casacore::IPosition start(3, 0, 0, z);
                casacore::IPosition length(3, shape[0], shape[1], 1);
                casacore::Slicer slicer(start, length);
                casacore::Array<casacore::Complex> slice;
                img.doGetSlice(slice, slicer);
            }
        }
        timings_out[2] = std::chrono::duration<double, std::milli>(Clock::now() - tr0).count();

        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in bench_plane_by_plane_complex");
        return -1;
    }
}

} // extern "C"
