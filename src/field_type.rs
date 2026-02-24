//! Segment V2 列的物理字段类型

use crate::common::ColumnType;

/// 列在 Segment 文件中的存储类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Int8, Int16, Int32, Int64,
    Float32, Float64,
    /// 变长字节（Varchar / String）
    Bytes,
    /// 日期存储为 i32（天数）
    Date,
}

impl From<ColumnType> for FieldType {
    fn from(ct: ColumnType) -> Self {
        match ct {
            ColumnType::Int8    => Self::Int8,
            ColumnType::Int16   => Self::Int16,
            ColumnType::Int32   => Self::Int32,
            ColumnType::Int64   => Self::Int64,
            ColumnType::Float32 => Self::Float32,
            ColumnType::Float64 => Self::Float64,
            ColumnType::Varchar => Self::Bytes,
            ColumnType::Date    => Self::Date,
        }
    }
}

impl FieldType {
    /// 固定字节宽度；变长类型返回 None
    pub fn fixed_size(self) -> Option<usize> {
        match self {
            Self::Int8               => Some(1),
            Self::Int16              => Some(2),
            Self::Int32 | Self::Date => Some(4),
            Self::Int64              => Some(8),
            Self::Float32            => Some(4),
            Self::Float64            => Some(8),
            Self::Bytes              => None,
        }
    }
    pub fn is_integer(self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::Date)
    }
}

/// 编码方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingType {
    Plain,
    RunLength,
    DeltaBinary,
    Dictionary,
}

/// 压缩方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Lz4,
}

/// 每列的元数据（写入 Footer）
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub column_id:   u32,
    pub name:        String,
    pub field_type:  FieldType,
    pub is_nullable: bool,
    pub encoding:    EncodingType,
    pub compression: CompressionType,
    pub max_length:  u32,
}

impl ColumnMeta {
    pub fn new(column_id: u32, name: &str, field_type: FieldType) -> Self {
        // 根据字段类型自动选择编码：有序整数用 Delta；字符串用字典；其余用 Plain
        let encoding = if field_type.is_integer() {
            EncodingType::DeltaBinary
        } else if field_type == FieldType::Bytes {
            EncodingType::Dictionary
        } else {
            EncodingType::Plain
        };
        Self {
            column_id, name: name.into(), field_type,
            is_nullable: false, encoding,
            compression: CompressionType::Lz4, max_length: 65535,
        }
    }

    pub fn with_encoding(mut self, enc: EncodingType) -> Self {
        self.encoding = enc; self
    }
    pub fn with_compression(mut self, comp: CompressionType) -> Self {
        self.compression = comp; self
    }
    pub fn nullable(mut self) -> Self {
        self.is_nullable = true; self
    }
}

/// 列值（运行时表示）
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int8(v)  => Some(*v as i64),
            Self::Int16(v) => Some(*v as i64),
            Self::Int32(v) => Some(*v as i64),
            Self::Int64(v) => Some(*v),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self { Self::Bytes(b) => Some(b), _ => None }
    }
    /// 将值序列化为用于索引比较的字节串
    pub fn to_sort_key(&self) -> Vec<u8> {
        match self {
            Self::Null       => vec![],
            Self::Int64(v)   => v.to_be_bytes().to_vec(),
            Self::Int32(v)   => v.to_be_bytes().to_vec(),
            Self::Float64(v) => v.to_bits().to_be_bytes().to_vec(),
            Self::Bytes(b)   => b.clone(),
            _                => format!("{:?}", self).into_bytes(),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null       => write!(f, "NULL"),
            Self::Int8(v)    => write!(f, "{v}"),
            Self::Int16(v)   => write!(f, "{v}"),
            Self::Int32(v)   => write!(f, "{v}"),
            Self::Int64(v)   => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Bytes(b)   => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}
