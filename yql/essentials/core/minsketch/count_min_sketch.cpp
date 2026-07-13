#include "count_min_sketch.h"

#include <util/system/compiler.h>
#include <util/generic/yexception.h>

#include <cstdlib>

namespace NKikimr {

TCountMinSketch* TCountMinSketch::Create(ui64 width, ui64 depth) {
    auto size = StaticSize(width, depth);
    auto* data = ::malloc(size);
    Y_ENSURE(data);
    auto* sketch = reinterpret_cast<TCountMinSketch*>(data);
    std::memset(sketch, 0, size);
    sketch->Width_ = width;
    sketch->Depth_ = depth;
    sketch->ElementCount_ = 0;
    return sketch;
}

TCountMinSketch* TCountMinSketch::FromString(const char* data, size_t size) {
    Y_ENSURE(size >= sizeof(TCountMinSketch));
    size_t bucketBytes = size - sizeof(TCountMinSketch);
    Y_ENSURE(bucketBytes % sizeof(ui32) == 0);

    auto* from = reinterpret_cast<const TCountMinSketch*>(data);
    ui64 width = from->Width_;
    ui64 depth = from->Depth_;
    Y_ENSURE(width != 0 && depth != 0);

    ui64 numBuckets = bucketBytes / sizeof(ui32);
    Y_ENSURE(width <= numBuckets && numBuckets % width == 0 && numBuckets / width == depth);

    auto* dataDst = ::malloc(size);
    Y_ENSURE(dataDst);

    std::memcpy(dataDst, data, size);
    return reinterpret_cast<TCountMinSketch*>(dataDst);
}

void TCountMinSketch::operator delete(void* data) noexcept {
    ::free(data);
}

ui64 TCountMinSketch::Hash(const char* data, size_t size, size_t hashIndex) {
    // fnv1a
    ui64 hash = 14695981039346656037ULL + 31 * hashIndex;
    const unsigned char* ptr = (const unsigned char*)data;
    for (size_t i = 0; i < size; ++i, ++ptr) {
        hash = hash ^ (*ptr);
        hash = hash * 1099511628211ULL;
    }
    return hash;
}

void TCountMinSketch::Count(const char* data, size_t size) {
    ui32* start = Buckets();
    for (size_t d = 0; d < Depth_; ++d, start += Width_) {
        ui64 hash = Hash(data, size, d);
        ui32* bucket = start + hash % Width_;
        if (Y_LIKELY(*bucket < std::numeric_limits<ui32>::max())) {
            ++*bucket;
        }
    }
    ++ElementCount_;
}

ui32 TCountMinSketch::Probe(const char* data, size_t size) const {
    ui32 minValue = std::numeric_limits<ui32>::max();
    const ui32* start = Buckets();
    for (size_t d = 0; d < Depth_; ++d, start += Width_) {
        ui64 hash = Hash(data, size, d);
        const ui32* bucket = start + hash % Width_;
        minValue = std::min(minValue, *bucket);
    }
    return minValue;
}

// Returns cardinality of overlapping keys based on PK domain bucket counts.
// NOTE: this method works given the same column domain, hashing method and seeds, as well as equal width and depth.
TMaybe<ui64> TCountMinSketch::GetOverlappingCardinality(const TCountMinSketch& rhs) const {
    if (Width_ != rhs.Width_ || Depth_ != rhs.Depth_) {
        return Nothing();
    }

    ui64 minCardinality = std::numeric_limits<ui64>::max();

    for (ui64 d = 0; d < Depth_; ++d) {
        ui64 cardinality = 0;
        for (ui64 w = 0; w < Width_; ++w) {
            ui64 idx = d * Width_ + w;
            cardinality += std::min(Buckets()[idx], rhs.Buckets()[idx]);
        }
        minCardinality = std::min(minCardinality, cardinality);
    }

    return minCardinality;
}

TCountMinSketch& TCountMinSketch::operator+=(const TCountMinSketch& rhs) {
    Y_ENSURE(Width_ == rhs.Width_ && Depth_ == rhs.Depth_);
    ui32* dst = Buckets();
    const ui32* src = rhs.Buckets();
    ui32* end = dst + Width_ * Depth_;
    for (; dst != end; ++dst, ++src) {
        ui32 sum = *dst + *src;
        if (Y_UNLIKELY(sum < *dst)) {
            *dst = std::numeric_limits<ui32>::max();
        } else {
            *dst = sum;
        }
    }
    ElementCount_ += rhs.ElementCount_;
    return *this;
}

} // namespace NKikimr
