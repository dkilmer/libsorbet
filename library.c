#include "library.h"
#include <stdlib.h>
#include <memory.h>
#include <math.h>

const int64_t SORBET_SIGNATURE = -3532510898378833984;
const uint8_t SORBET_VERSION = 3;

void sorbet_flush_write_buffer_uncompressed(sorbet_def_t *sdef) {
	if (sdef->buf_offset <= 0) return;
	size_t written = fwrite(sdef->buf, sizeof(uint8_t), sdef->buf_offset, sdef->f);
	if (written != sdef->buf_offset) {
		printf("ERROR: asked to write %d bytes but wrote %d\n", sdef->buf_offset, (int)written);
	}
	sdef->buf_offset = 0;
}

void sorbet_flush_write_buffer_compressed(sorbet_def_t *sdef) {
	if (sdef->buf_offset <= 0) return;
	sdef->zstrm.avail_in = sdef->buf_offset;
	sdef->zstrm.next_in = sdef->buf;
	do {
		sdef->zstrm.avail_out = BUF_SIZE;
		sdef->zstrm.next_out = sdef->zout;
		int ret = deflate(&sdef->zstrm, sdef->zflush);
		int have = BUF_SIZE - sdef->zstrm.avail_out;
		size_t written = fwrite(sdef->zout, sizeof(uint8_t), have, sdef->f);
		if (written != have) {
			printf("ERROR: asked to write %d bytes but wrote %d\n", have, (int)written);
		}
	} while (sdef->zstrm.avail_out == 0);
	if (sdef->zflush == Z_FINISH) {
		deflateEnd(&sdef->zstrm);
	}
	sdef->buf_offset = 0;
}

void sorbet_flush_write_buffer(sorbet_def_t *sdef) {
	if (sdef->buf_offset <= 0) return;
	if (sdef->compression == 0) {
		sorbet_flush_write_buffer_uncompressed(sdef);
	} else {
		sorbet_flush_write_buffer_compressed(sdef);
	}
}

void sorbet_write_type_tag(sorbet_def_t *sdef, column_type_t type) {
	if ((sdef->buf_offset + 1) > sdef->buf_size) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_tag[type];
	sdef->buf_offset += 1;
	sdef->uc_size += 1;
}

void sorbet_write_null_type_tag(sorbet_def_t *sdef, column_type_t type) {
	if ((sdef->buf_offset + 1) > sdef->buf_size) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_null_tag[type];
	sdef->cstats[sdef->cur_col].cnulls++;
	sdef->buf_offset += 1;
	sdef->uc_size += 1;
}

void sorbet_write_bytes_raw(sorbet_def_t *sdef, const uint8_t *v, int32_t len) {
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	if ((sdef->buf_offset + len) < sdef->buf_size) {
		// the whole thing fits in the buffer
		for (int i=0; i<len; i++) {
			tbuf[i] = v[i];
		}
		sdef->buf_offset += len;
	} else {
		// need to span multiple buffers
		uint8_t *pv = (uint8_t *)v;
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
	sdef->uc_size += len;
}

void sorbet_write_byte_raw(sorbet_def_t *sdef, uint8_t v) {
	sorbet_write_bytes_raw(sdef, &v, 1);
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

void inc_col(sorbet_def_t *sdef) {
	sdef->cur_col++;
	if (sdef->cur_col >= sdef->schema.numCols) {
		sdef->cur_col = 0;
		sdef->n_rows++;
	}
}

void sorbet_write_int(sorbet_def_t *sdef, const int32_t *v) {
	if (v != NULL) {
		if (abs(*v) > sdef->cstats[sdef->cur_col].max_int) sdef->cstats[sdef->cur_col].max_int = *v;
		sorbet_write_type_tag(sdef, column_type_tag[INTEGER]);
		sorbet_write_int_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[INTEGER]);
	}
	inc_col(sdef);
}

void sorbet_write_long(sorbet_def_t *sdef, const int64_t *v) {
	if (v != NULL) {
		if (labs(*v) > sdef->cstats[sdef->cur_col].max_long) sdef->cstats[sdef->cur_col].max_long = *v;
		sorbet_write_type_tag(sdef, column_type_tag[LONG]);
		sorbet_write_long_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[LONG]);
	}
	inc_col(sdef);
}

void sorbet_write_float(sorbet_def_t *sdef, const float32_t *v) {
	if (v != NULL) {
		if (fabsf(*v) > sdef->cstats[sdef->cur_col].max_float) sdef->cstats[sdef->cur_col].max_float = *v;
		sorbet_write_type_tag(sdef, column_type_tag[FLOAT]);
		sorbet_write_float_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[FLOAT]);
	}
	inc_col(sdef);
}

void sorbet_write_double(sorbet_def_t *sdef, const float64_t *v) {
	if (v != NULL) {
		if (fabs(*v) > sdef->cstats[sdef->cur_col].max_double) sdef->cstats[sdef->cur_col].max_double = *v;
		sorbet_write_type_tag(sdef, column_type_tag[DOUBLE]);
		sorbet_write_double_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DOUBLE]);
	}
	inc_col(sdef);
}

void sorbet_write_boolean(sorbet_def_t *sdef, const bool *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[BOOLEAN]);
		int32_t bv = (*v) ? 1 : 0;
		sorbet_write_int_raw(sdef, bv);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BOOLEAN]);
	}
	inc_col(sdef);
}

void sorbet_write_string(sorbet_def_t *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		if (len > sdef->cstats[sdef->cur_col].cwidth) sdef->cstats[sdef->cur_col].cwidth = len;
		sorbet_write_type_tag(sdef, column_type_tag[STRING]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[STRING]);
	}
	inc_col(sdef);
}

void sorbet_write_binary(sorbet_def_t *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		if (len > sdef->cstats[sdef->cur_col].cwidth) sdef->cstats[sdef->cur_col].cwidth = len;
		sorbet_write_type_tag(sdef, column_type_tag[BINARY]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BINARY]);
	}
	inc_col(sdef);
}

void sorbet_write_date(sorbet_def_t *sdef, const sorbet_date_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATE]);
		int32_t dt = (v->y * 10000) + (v->m * 100) + (v->d);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATE]);
	}
	inc_col(sdef);
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
	inc_col(sdef);
}

void sorbet_write_datetime(sorbet_def_t *sdef, const int64_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, *dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
	inc_col(sdef);
}

void sorbet_write_datetime_time_t(sorbet_def_t *sdef, const time_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, (int64_t )*dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
	inc_col(sdef);
}

void sorbet_write_time(sorbet_def_t *sdef, const sorbet_time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[TIME]);
		int32_t dt = (v->h * 10000) + (v->m * 100) + (v->s);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[TIME]);
	}
	inc_col(sdef);
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
	inc_col(sdef);
}

int64_t col_width_from_stats(column_stats_t *stats, column_type_t col_type) {
	int64_t max = 0L;
	char strbuf[256];
	switch (col_type) {
		case INTEGER: {
			sprintf(strbuf, "%d", stats->max_int);
			max = strlen(strbuf);
			break;
		}
		case LONG: {
			sprintf(strbuf, "%ld", stats->max_long);
			max = strlen(strbuf);
			break;
		}
		case FLOAT: {
			sprintf(strbuf, "%d", (int32_t )stats->max_float);
			max = strlen(strbuf);
			break;
		}
		case DOUBLE: {
			sprintf(strbuf, "%ld", (int64_t)stats->max_long);
			max = strlen(strbuf);
			break;
		}
		case STRING:
		case BINARY: {
			max = stats->cwidth;
			break;
		}
	}
	return max;
}

void write_header(sorbet_def_t *sdef) {
	sorbet_write_long_raw(sdef, SORBET_SIGNATURE);
	sorbet_write_byte_raw(sdef, SORBET_VERSION);
	// compression type
	sorbet_write_byte_raw(sdef, sdef->compression);
	// number of rows
	sorbet_write_long_raw(sdef, sdef->n_rows);
	// uncompressed size including header
	sorbet_write_long_raw(sdef, sdef->uc_size);
	sorbet_write_int_raw(sdef, sdef->schema.numCols);
	for (int i = 0; i < sdef->schema.numCols; i++) {
		data_column_t dc = sdef->schema.cols[i];
		column_stats_t st = sdef->cstats[i];
		int32_t name_len = strlen(dc.name);
		sorbet_write_int_raw(sdef,name_len);
		sorbet_write_bytes_raw(sdef, (uint8_t *)dc.name, name_len);
		sorbet_write_byte_raw(sdef, dc.type);
		sorbet_write_byte_raw(sdef, dc.valType);
		sorbet_write_byte_raw(sdef, dc.keyType);
		// maximum val width
		sorbet_write_int_raw(sdef, col_width_from_stats(&st, sdef->schema.cols[i].type));
		// number of nulls
		sorbet_write_long_raw(sdef, st.cnulls);
		// number of bads
		sorbet_write_long_raw(sdef, st.cbads);
	}
}

void write_metadata(sorbet_def_t *sdef, int metadataType, int metadataSize, const uint8_t *metadata) {
	if (metadataSize > 0 && metadata != NULL) {
		sorbet_write_int_raw(sdef, metadataType);
		sorbet_write_int_raw(sdef, metadataSize);
		sorbet_write_bytes_raw(sdef, metadata, metadataSize);
	} else {
		sorbet_write_int_raw(sdef, 0);
		sorbet_write_int_raw(sdef, 0);
	}
}

sorbet_def_t *sorbet_writer_open(
		const char *filename,
		schema_t schema,
		bool compressed,
		int metadataType,
		int metadataSize,
		const uint8_t *metadata
) {
	FILE *f = fopen(filename, "wb");
	sorbet_def_t *sdef = malloc(sizeof(sorbet_def_t));
	sdef->f = f;
	sdef->filename = filename;
	sdef->schema = schema;
	sdef->buf_size = BUF_SIZE;
	//sdef->buf = (uint8_t *)malloc(BUF_SIZE);
	sdef->buf_offset = 0;
	sdef->uc_size = 0;
	sdef->n_rows = 0;
	sdef->cstats = (column_stats_t *)malloc(schema.numCols * sizeof(column_stats_t));
	sdef->cur_col = 0;
	sdef->zflush = Z_NO_FLUSH;
	if (compressed) {
		sdef->compression = 1;
		sdef->zstrm.zalloc = Z_NULL;
		sdef->zstrm.zfree = Z_NULL;
		sdef->zstrm.opaque = Z_NULL;
		int ret = deflateInit2(&sdef->zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, Z_WINDOW_BITS | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY);
		if (ret != Z_OK) {
			printf("ERROR: deflateInit returned %d\n", ret);
			sdef->compression = 0;
		}
	} else {
		sdef -> compression = 0;
	}
	write_header(sdef);
	write_metadata(sdef, metadataType, metadataSize, metadata);
	// header and metadata are not compressed
	sorbet_flush_write_buffer_uncompressed(sdef);
	return sdef;
}

void sorbet_writer_close(sorbet_def_t *sdef) {
	sdef->zflush = Z_FINISH;
	sorbet_flush_write_buffer(sdef);
	fseeko(sdef->f, 0, 0);
	write_header(sdef);
	// header and metadata are not compressed
	sorbet_flush_write_buffer_uncompressed(sdef);
	fclose(sdef->f);
	free(sdef->cstats);
	free(sdef);
}
