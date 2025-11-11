pub mod encoding;

pub use encoding::{
    encode_record_batches, set_parquet_row_group_size, writer_properties, EncodedParquet,
};
