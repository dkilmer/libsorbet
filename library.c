#include "library.h"
#include <stdlib.h>
#include <memory.h>

sorbet_def_t *sorbet_writer_open(
		const char *filename,
		schema_t schema,
		bool compressed,
		int metadataType,
		int metadataSize,
		const unsigned char * metadata
) {
/*
	for (int i = 0; i < schema.numCols; i++) {
		struct DataColumn dc = schema.cols[i];
		printf("%s %s\n", dc.name, column_type_label[dc.type]);
	}
*/
	FILE *f = fopen(filename, "wb");

	sorbet_def_t *sdef = malloc(sizeof(sorbet_def_t));
	sdef->f = f;
	sdef->filename = filename;
	sdef->schema = schema;
	sdef->buf_size = BUF_SIZE;
	sdef->buf = (uint8_t *)malloc(BUF_SIZE);
	sdef->buf_offset = 0;
	return sdef;
}

void sorbet_flush_write_buffer(sorbet_def_t *sdef) {
	if (sdef->buf_offset <= 0) return;
	size_t written = fwrite(sdef->buf, sizeof(uint8_t), sdef->buf_offset, sdef->f);
	if (written != sdef->buf_offset) {
		printf("ERROR: asked to write %d bytes but wrote %d\n", sdef->buf_offset, (int)written);
	}
	sdef->buf_offset = 0;
}

void sorbet_writer_close(sorbet_def_t *sdef) {
	sorbet_flush_write_buffer(sdef);
	fclose(sdef->f);
	free(sdef->buf);
	free(sdef);
}

void sorbet_write_type_tag(sorbet_def_t *sdef, column_type_t type) {
	if ((sdef->buf_offset + 1) > sdef->buf_size) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_tag[type];
	sdef->buf_offset += 1;
}

void sorbet_write_null_type_tag(sorbet_def_t *sdef, column_type_t type) {
	if ((sdef->buf_offset + 1) > sdef->buf_size) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_null_tag[type];
	sdef->buf_offset += 1;
}

void sorbet_write_bytes_raw(sorbet_def_t *sdef, uint8_t *v, int32_t len) {
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	if ((sdef->buf_offset + len) < sdef->buf_size) {
		// the whole thing fits in the buffer
		for (int i=0; i<len; i++) {
			tbuf[i] = v[i];
		}
		sdef->buf_offset += len;
	} else {
		// need to span multiple buffers
		uint8_t *pv = v;
		int plen = len;
		// copy to the end of the current buffer and flush
		int remainingLen = BUF_SIZE-sdef->buf_offset;
		for (int i=0; i<remainingLen; i++) {
			tbuf[i] = pv[i];
		}
		sdef->buf_offset += remainingLen;
		sorbet_flush_write_buffer(sdef);
		pv += remainingLen;
		plen -= remainingLen;
		// write
		while (plen > 0) {
			tbuf = sdef->buf;
			int bytesToWrite = (plen >= BUF_SIZE) ? BUF_SIZE : plen;
			for (int i=0; i<bytesToWrite; i++) {
				tbuf[i] = pv[i];
			}
			if (bytesToWrite == BUF_SIZE) {
				sdef->buf_offset = BUF_SIZE;
				sorbet_flush_write_buffer(sdef);
			} else {
				sdef->buf_offset += bytesToWrite;
			}
			pv += bytesToWrite;
			plen -= bytesToWrite;
		}
	}
}

void sorbet_write_int_raw(sorbet_def_t *sdef, int32_t v) {
	union {
		int32_t v;
		uint8_t bytes[4];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 4);
}

void sorbet_write_long_raw(sorbet_def_t *sdef, int64_t v) {
	union {
		int64_t v;
		uint8_t bytes[8];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 8);
}

void sorbet_write_float_raw(sorbet_def_t *sdef, float32_t v) {
	union {
		float32_t v;
		uint8_t bytes[4];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 4);
}

void sorbet_write_double_raw(sorbet_def_t *sdef, float64_t v) {
	union {
		float64_t v;
		uint8_t bytes[8];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 8);
}

void sorbet_write_int(sorbet_def_t *sdef, const int32_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[INTEGER]);
		sorbet_write_int_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[INTEGER]);
	}
}

void sorbet_write_long(sorbet_def_t *sdef, const int64_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[LONG]);
		sorbet_write_long_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[LONG]);
	}
}

void sorbet_write_float(sorbet_def_t *sdef, const float32_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[FLOAT]);
		sorbet_write_float_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[FLOAT]);
	}
}

void sorbet_write_double(sorbet_def_t *sdef, const float64_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DOUBLE]);
		sorbet_write_double_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DOUBLE]);
	}
}

void sorbet_write_boolean(sorbet_def_t *sdef, const bool *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[BOOLEAN]);
		int32_t bv = (*v) ? 1 : 0;
		sorbet_write_int_raw(sdef, bv);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BOOLEAN]);
	}
}

void sorbet_write_string(sorbet_def_t *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[STRING]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[STRING]);
	}
}

void sorbet_write_binary(sorbet_def_t *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[BINARY]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BINARY]);
	}
}

void sorbet_write_date(sorbet_def_t *sdef, const sorbet_date_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATE]);
		int32_t dt = (v->y * 10000) + (v->m * 100) + (v->d);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATE]);
	}
}

void sorbet_write_date_time_t(sorbet_def_t *sdef, const time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATE]);
		struct tm *ltime = localtime(v);
		int32_t dt = (ltime->tm_year * 10000) + ((ltime->tm_mon+1) * 100) + (ltime->tm_mday);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATE]);
	}
}

void sorbet_write_datetime(sorbet_def_t *sdef, const int64_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, *dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
}

void sorbet_write_datetime_time_t(sorbet_def_t *sdef, const time_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, (int64_t )*dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
}

void sorbet_write_time(sorbet_def_t *sdef, const sorbet_time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[TIME]);
		int32_t dt = (v->h * 10000) + (v->m * 100) + (v->s);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[TIME]);
	}
}

void sorbet_write_time_time_t(sorbet_def_t *sdef, const time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[TIME]);
		struct tm *ltime = localtime(v);
		int32_t dt = (ltime->tm_hour * 10000) + (ltime->tm_min * 100) + (ltime->tm_sec);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[TIME]);
	}
}
