use std::io::{self, ErrorKind};

use zstd_safe::{zstd_sys::ZSTD_EndDirective, InBuffer, OutBuffer};

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

    pub fn set_long(&mut self) -> io::Result<()> {
        self.cctx
            .set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(true))
            .map_err(map_zstd_error)?;
        Ok(())
    }

    pub fn compress(&mut self, buf: &[u8], out: &mut Vec<u8>) -> io::Result<usize> {
        let mut in_buf = InBuffer::around(buf);
        let mut out_buf = OutBuffer::around_pos(out, out.len());
        self.cctx
            .compress_stream2(
                &mut out_buf,
                &mut in_buf,
                ZSTD_EndDirective::ZSTD_e_continue,
            )
            .map_err(map_zstd_error)?;
        Ok(in_buf.pos())
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
