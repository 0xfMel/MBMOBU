use std::io::{self, Cursor, ErrorKind};

use zstd_safe::{zstd_sys::ZSTD_EndDirective, InBuffer, OutBuffer};

use crate::util;

pub struct Zstd<'a> {
    cctx: zstd_safe::CCtx<'a>,
}

impl Zstd<'_> {
    pub fn new() -> Self {
        Self {
            cctx: zstd_safe::CCtx::create(),
        }
    }

    pub fn set_level(&mut self, level: i32) -> io::Result<()> {
        self.cctx
            .set_parameter(zstd_safe::CParameter::CompressionLevel(level))
            .map_err(map_zstd_error)?;
        Ok(())
    }

    pub fn set_long(&mut self, long: bool) -> io::Result<()> {
        self.cctx
            .set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(long))
            .map_err(map_zstd_error)?;
        Ok(())
    }

    pub fn compress(&mut self, src: &mut Cursor<&[u8]>, out: &mut Vec<u8>) -> io::Result<()> {
        let mut in_buf = InBuffer::around(src.get_ref());
        in_buf.set_pos(
            src.position()
                .try_into()
                .expect("src position should be small enough to fit in a usize"),
        );
        let mut out_buf = OutBuffer::around_pos(out, out.len());
        self.cctx
            .compress_stream2(
                &mut out_buf,
                &mut in_buf,
                ZSTD_EndDirective::ZSTD_e_continue,
            )
            .map_err(map_zstd_error)?;
        src.set_position(util::usize_to_u64(in_buf.pos()));
        Ok(())
    }

    pub fn end_frame(&mut self, out: &mut Vec<u8>) -> io::Result<usize> {
        let mut in_buf = InBuffer::around(&[]);
        let mut out_buf = OutBuffer::around_pos(out, out.len());
        let remaining = self
            .cctx
            .compress_stream2(&mut out_buf, &mut in_buf, ZSTD_EndDirective::ZSTD_e_end)
            .map_err(map_zstd_error)?;
        Ok(remaining)
    }
}

fn map_zstd_error(code: usize) -> io::Error {
    let msg = zstd_safe::get_error_name(code);
    io::Error::new(ErrorKind::Other, msg.to_owned())
}
