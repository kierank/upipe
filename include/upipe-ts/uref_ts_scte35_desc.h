/*
 * Copyright (C) 2020-2025 EasyTools
 *
 * Authors: Arnaud de Turckheim
 *
 * SPDX-License-Identifier: MIT
 */

/** @file
 * @short Upipe uref attributes for TS SCTE 35 descriptors
 */

#ifndef _UPIPE_TS_UREF_TS_SCTE35_DESC_H_
/** @hidden */
#define _UPIPE_TS_UREF_TS_SCTE35_DESC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "upipe/uref.h"
#include "upipe/uref_attr.h"

#include <inttypes.h>
#include <string.h>
#include <stdint.h>

#ifndef UREF_COMMA
/** @hidden: used to pass multiple args through single-parameter macro slots */
#define UREF_COMMA ,
#endif

/* splice descriptor */
UREF_ATTR_SMALL_UNSIGNED(ts_scte35_desc, tag, "scte35.desc.tag", tag)
UREF_ATTR_UNSIGNED(ts_scte35_desc, identifier, "scte35.desc.id", identifier)

/* avail splice descriptor */
UREF_ATTR_UNSIGNED(ts_scte35_desc_avail, provider_avail_id,
                   "scte35.desc.avail.provider_avail_id", provider avail id)

/* segmentation splice descriptor - indexed by descriptor position n */
UREF_ATTR_UNSIGNED_VA(ts_scte35_desc_seg, event_id,
                      "scte35.desc.seg[%" PRIu64 "].event_id",
                      segmentation event id, uint64_t n, n)
UREF_ATTR_VOID_VA(ts_scte35_desc_seg, cancel,
                  "scte35.desc.seg[%" PRIu64 "].cancel",
                  segmentation event cancel indicator, uint64_t n, n)
UREF_ATTR_VOID_VA(ts_scte35_desc_seg, delivery_not_restricted,
                  "scte35.desc.seg[%" PRIu64 "].delivery_not_restricted",
                  delivery not restricted, uint64_t n, n)
UREF_ATTR_VOID_VA(ts_scte35_desc_seg, web,
                  "scte35.desc.seg[%" PRIu64 "].web",
                  web delivery allowed, uint64_t n, n)
UREF_ATTR_VOID_VA(ts_scte35_desc_seg, no_regional_blackout,
                  "scte35.desc.seg[%" PRIu64 "].no_regional_blackout",
                  no regional blackout, uint64_t n, n)
UREF_ATTR_VOID_VA(ts_scte35_desc_seg, archive,
                  "scte35.desc.seg[%" PRIu64 "].archive",
                  archive allowed, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, device,
                             "scte35.desc.seg[%" PRIu64 "].device",
                             device restrictions, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, nb_comp,
                             "scte35.desc.seg[%" PRIu64 "].nb_comp",
                             component count, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg_comp, tag,
                             "scte35.desc.seg[%" PRIu64 "].comp[%u].tag",
                             component tag,
                             uint64_t n UREF_COMMA uint8_t comp,
                             n UREF_COMMA comp)
UREF_ATTR_UNSIGNED_VA(ts_scte35_desc_seg_comp, pts_off,
                      "scte35.desc.seg[%" PRIu64 "].comp[%u].pts_off",
                      component PTS offset,
                      uint64_t n UREF_COMMA uint8_t comp,
                      n UREF_COMMA comp)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, upid_type,
                             "scte35.desc.seg[%" PRIu64 "].upid_type",
                             segmentation upid type, uint64_t n, n)
UREF_ATTR_STRING_VA(ts_scte35_desc_seg, upid_type_name,
                    "scte35.desc.seg[%" PRIu64 "].upid_type_name",
                    segmentation upid type name, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, upid_length,
                             "scte35.desc.seg[%" PRIu64 "].upid_length",
                             segmentation upid length, uint64_t n, n)
UREF_ATTR_OPAQUE_VA(ts_scte35_desc_seg, upid,
                    "scte35.desc.seg[%" PRIu64 "].upid",
                    segmentation upid, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, type_id,
                             "scte35.desc.seg[%" PRIu64 "].type_id",
                             segmentation type id, uint64_t n, n)
UREF_ATTR_STRING_VA(ts_scte35_desc_seg, type_id_name,
                    "scte35.desc.seg[%" PRIu64 "].type_id_name",
                    segmentation type id name, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, num,
                             "scte35.desc.seg[%" PRIu64 "].num",
                             segment num, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, expected,
                             "scte35.desc.seg[%" PRIu64 "].expected",
                             segments expected, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, sub_num,
                             "scte35.desc.seg[%" PRIu64 "].sub_num",
                             sub segment num, uint64_t n, n)
UREF_ATTR_SMALL_UNSIGNED_VA(ts_scte35_desc_seg, sub_expected,
                             "scte35.desc.seg[%" PRIu64 "].sub_expected",
                             sub segment expected, uint64_t n, n)

/* time splice descriptor */
UREF_ATTR_UNSIGNED(ts_scte35_desc_time, tai_sec, "scte35.desc.time.tai_sec",
                   seconds part of the TAI);
UREF_ATTR_UNSIGNED(ts_scte35_desc_time, tai_nsec, "scte35.desc.time.tai_nsec",
                   nanoseconds part of the TAI);
UREF_ATTR_UNSIGNED(ts_scte35_desc_time, utc_off, "scte35.desc.time.utc_off",
                   offset from UTC in seconds);

#ifdef __cplusplus
}
#endif
#endif
