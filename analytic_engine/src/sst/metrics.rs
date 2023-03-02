// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram, Histogram};

lazy_static! {
    // Histogram:
    // Buckets: 100B,200B,400B,...,2KB
    pub static ref SST_GET_RANGE_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_length",
        "Histogram for sst get range length",
        exponential_buckets(100.0, 2.0, 5).unwrap()
    ).unwrap();

    // Histogram:
    // Buckets: 100B,200B,400B,...,2KB
    pub static ref SST_GET_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_duration",
        "Histogram for sst_get_range duration of the table in seconds",
        exponential_buckets(0.002, 4.0, 10).unwrap()
    ).unwrap();
}
