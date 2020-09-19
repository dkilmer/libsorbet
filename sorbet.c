#include "sorbet.h"
#include <stdlib.h>
#include <memory.h>
#include <math.h>
#include <assert.h>

const int64_t SORBET_SIGNATURE = -3532510898378833984;
const uint8_t SORBET_VERSION = 3;

int sorbet_version() {
	return SORBET_VERSION;
}

void sorbet_flush_write_buffer_uncompressed(sorbet_def *sdef) {
	if (sdef->buf_offset <= 0) return;
	size_t written = fwrite(sdef->buf, sizeof(uint8_t), sdef->buf_offset, sdef->f);
	if (written != sdef->buf_offset) {
		printf("ERROR: asked to write %d bytes but wrote %d\n", sdef->buf_offset, (int)written);
	}
	sdef->buf_offset = 0;
}

void sorbet_flush_write_buffer_compressed(sorbet_def *sdef) {
	if (sdef->buf_offset <= 0) return;
	sdef->zstrm.avail_in = sdef->buf_offset;
	sdef->zstrm.next_in = sdef->buf;
	do {
		sdef->zstrm.avail_out = BUF_SIZE;
		sdef->zstrm.next_out = sdef->zbuf;
		int ret = deflate(&sdef->zstrm, sdef->zflush);
		int have = BUF_SIZE - sdef->zstrm.avail_out;
		size_t written = fwrite(sdef->zbuf, sizeof(uint8_t), have, sdef->f);
		if (written != have) {
			printf("ERROR: asked to write %d bytes but wrote %d\n", have, (int)written);
		}
	} while (sdef->zstrm.avail_out == 0);
	if (sdef->zflush == Z_FINISH) {
		deflateEnd(&sdef->zstrm);
	}
	sdef->buf_offset = 0;
}

void sorbet_flush_write_buffer(sorbet_def *sdef) {
	if (sdef->buf_offset <= 0) return;
	if (sdef->compression == 0) {
		sorbet_flush_write_buffer_uncompressed(sdef);
	} else {
		sorbet_flush_write_buffer_compressed(sdef);
	}
}

void sorbet_write_type_tag(sorbet_def *sdef, column_type type) {
	if ((sdef->buf_offset + 1) > BUF_SIZE) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_tag[type];
	sdef->buf_offset += 1;
	sdef->uc_size += 1;
}

void sorbet_write_null_type_tag(sorbet_def *sdef, column_type type) {
	if ((sdef->buf_offset + 1) > BUF_SIZE) {
		sorbet_flush_write_buffer(sdef);
	}
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	tbuf[0] = column_type_null_tag[type];
	sdef->cstats[sdef->cur_col].cnulls++;
	sdef->buf_offset += 1;
	sdef->uc_size += 1;
}

void sorbet_write_bytes_raw(sorbet_def *sdef, const uint8_t *v, int32_t len) {
	uint8_t *tbuf = sdef->buf + sdef->buf_offset;
	if ((sdef->buf_offset + len) < BUF_SIZE) {
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

void sorbet_write_byte_raw(sorbet_def *sdef, uint8_t v) {
	sorbet_write_bytes_raw(sdef, &v, 1);
}

void sorbet_write_int_raw(sorbet_def *sdef, int32_t v) {
	union {
		int32_t v;
		uint8_t bytes[4];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 4);
}

void sorbet_write_long_raw(sorbet_def *sdef, int64_t v) {
	union {
		int64_t v;
		uint8_t bytes[8];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 8);
}

void sorbet_write_float_raw(sorbet_def *sdef, float32_t v) {
	union {
		float32_t v;
		uint8_t bytes[4];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 4);
}

void sorbet_write_double_raw(sorbet_def *sdef, float64_t v) {
	union {
		float64_t v;
		uint8_t bytes[8];
	} uv;
	uv.v = v;
	sorbet_write_bytes_raw(sdef, uv.bytes, 8);
}

void writer_inc_col(sorbet_def *sdef) {
	sdef->cur_col++;
	if (sdef->cur_col >= sdef->schema.numCols) {
		sdef->cur_col = 0;
		sdef->n_rows++;
	}
}

void sorbet_write_int(sorbet_def *sdef, const int32_t *v) {
	if (v != NULL) {
		if (abs(*v) > sdef->cstats[sdef->cur_col].max_int) sdef->cstats[sdef->cur_col].max_int = *v;
		sorbet_write_type_tag(sdef, column_type_tag[INTEGER]);
		sorbet_write_int_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[INTEGER]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_long(sorbet_def *sdef, const int64_t *v) {
	if (v != NULL) {
		if (labs(*v) > sdef->cstats[sdef->cur_col].max_long) sdef->cstats[sdef->cur_col].max_long = *v;
		sorbet_write_type_tag(sdef, column_type_tag[LONG]);
		sorbet_write_long_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[LONG]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_float(sorbet_def *sdef, const float32_t *v) {
	if (v != NULL) {
		if (fabsf(*v) > sdef->cstats[sdef->cur_col].max_float) sdef->cstats[sdef->cur_col].max_float = *v;
		sorbet_write_type_tag(sdef, column_type_tag[FLOAT]);
		sorbet_write_float_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[FLOAT]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_double(sorbet_def *sdef, const float64_t *v) {
	if (v != NULL) {
		if (fabs(*v) > sdef->cstats[sdef->cur_col].max_double) sdef->cstats[sdef->cur_col].max_double = *v;
		sorbet_write_type_tag(sdef, column_type_tag[DOUBLE]);
		sorbet_write_double_raw(sdef, *v);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DOUBLE]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_boolean(sorbet_def *sdef, const bool *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[BOOLEAN]);
		uint8_t bv = (*v) ? 1 : 0;
		sorbet_write_byte_raw(sdef, bv);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BOOLEAN]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_string(sorbet_def *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		if (len > sdef->cstats[sdef->cur_col].cwidth) sdef->cstats[sdef->cur_col].cwidth = len;
		sorbet_write_type_tag(sdef, column_type_tag[STRING]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[STRING]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_binary(sorbet_def *sdef, const uint8_t *v, int32_t len) {
	if (v != NULL) {
		if (len > sdef->cstats[sdef->cur_col].cwidth) sdef->cstats[sdef->cur_col].cwidth = len;
		sorbet_write_type_tag(sdef, column_type_tag[BINARY]);
		sorbet_write_int_raw(sdef, len);
		sorbet_write_bytes_raw(sdef, v, len);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[BINARY]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_date(sorbet_def *sdef, const sorbet_date *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATE]);
		int32_t dt = (v->y * 10000) + (v->m * 100) + (v->d);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATE]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_date_time_t(sorbet_def *sdef, const time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATE]);
		struct tm *ltime = localtime(v);
		int32_t dt = (ltime->tm_year * 10000) + ((ltime->tm_mon+1) * 100) + (ltime->tm_mday);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATE]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_datetime(sorbet_def *sdef, const int64_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, *dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_datetime_time_t(sorbet_def *sdef, const time_t *dt) {
	if (dt != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[DATETIME]);
		sorbet_write_long_raw(sdef, (int64_t )*dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[DATETIME]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_time(sorbet_def *sdef, const sorbet_time *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[TIME]);
		int32_t dt = (v->h * 10000) + (v->m * 100) + (v->s);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[TIME]);
	}
	writer_inc_col(sdef);
}

void sorbet_write_time_time_t(sorbet_def *sdef, const time_t *v) {
	if (v != NULL) {
		sorbet_write_type_tag(sdef, column_type_tag[TIME]);
		struct tm *ltime = localtime(v);
		int32_t dt = (ltime->tm_hour * 10000) + (ltime->tm_min * 100) + (ltime->tm_sec);
		sorbet_write_int_raw(sdef, dt);
	} else {
		sorbet_write_null_type_tag(sdef, column_type_null_tag[TIME]);
	}
	writer_inc_col(sdef);
}

int64_t col_width_from_stats(column_stats *stats, column_type col_type) {
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
			sprintf(strbuf, "%ld", (int64_t)stats->max_double);
			max = strlen(strbuf);
			break;
		}
		case STRING:
		case BINARY: {
			max = stats->cwidth;
			break;
		}
		default: {
			max = 0L;
		}
	}
	return max;
}

void write_header(sorbet_def *sdef) {
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
		data_column dc = sdef->schema.cols[i];
		column_stats st = sdef->cstats[i];
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

void write_metadata(sorbet_def *sdef) {
	if (sdef->metadataSize > 0 && sdef->metadata != NULL) {
		sorbet_write_int_raw(sdef, sdef->metadataType);
		sorbet_write_int_raw(sdef, sdef->metadataSize);
		sorbet_write_bytes_raw(sdef, sdef->metadata, sdef->metadataSize);
	} else {
		sorbet_write_int_raw(sdef, 0);
		sorbet_write_int_raw(sdef, 0);
	}
}

void sorbet_writer_open(sorbet_def *sdef) {
	sdef->f = fopen(sdef->filename, "wb");
	sdef->buf_size = BUF_SIZE;
	sdef->buf_offset = 0;
	sdef->uc_size = 0;
	sdef->n_rows = 0;
	sdef->cstats = (column_stats *)malloc(sdef->schema.numCols * sizeof(column_stats));
	sdef->cur_col = 0;
	sdef->zflush = Z_NO_FLUSH;
	if (sdef->compression == 1) {
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
	write_metadata(sdef);
	// header and metadata are not compressed
	sorbet_flush_write_buffer_uncompressed(sdef);
}

void sorbet_writer_close(sorbet_def *sdef) {
	sdef->zflush = Z_FINISH;
	sorbet_flush_write_buffer(sdef);
	fseeko(sdef->f, 0, 0);
	write_header(sdef);
	// header and metadata are not compressed
	sorbet_flush_write_buffer_uncompressed(sdef);
	fclose(sdef->f);
	free(sdef->cstats);
}

void sorbet_fill_read_buffer_uncompressed(sorbet_def *sdef) {
	if (sdef->buf_size < BUF_SIZE) return; // TODO: throw an error?
	if (sdef->buf_offset == 0) return; // we haven't used any of the buffer yet
	if (sdef->buf_offset < BUF_SIZE) {
		// we need to read a partial buffer
		uint8_t *src = sdef->buf + sdef->buf_offset;
		// move the tail of the buffer to the beginning
		int left = BUF_SIZE-sdef->buf_offset;
		memmove(sdef->buf, src, left);
		// read in the remainder of the buffer
		uint8_t *dst = sdef->buf + left;
		int bytes_read = (int)fread(dst, sizeof(uint8_t), BUF_SIZE-left, sdef->f);
		if (bytes_read < (BUF_SIZE-left)) {
			sdef->buf_size = left + bytes_read;
		}
	} else {
		// We're at the very end -- we can just read the whole buffer
		int bytes_read = (int)fread(sdef->buf, sizeof(uint8_t), BUF_SIZE, sdef->f);
		if (bytes_read < BUF_SIZE) {
			sdef->buf_size = bytes_read;
		}
	}
	sdef->buf_offset = 0;
}

void sorbet_fill_read_buffer_compressed(sorbet_def *sdef) {
	if (sdef->buf_offset == 0) {
		printf("\n-=-= returning from sorbet_fill_read_buffer_compressed because buf_offset is 0\n");
		return; // we haven't used any of the buffer yet
	}
	printf("\n-=-=sorbet_fill_read_buffer_compressed\n");
	uint8_t *dst = sdef->buf;
	int bytes_needed = BUF_SIZE;
	if (sdef->buf_offset < BUF_SIZE) {
		// we need to read a partial buffer
		uint8_t *src = sdef->buf + sdef->buf_offset;
		// move the tail of the buffer to the beginning
		int left = BUF_SIZE-sdef->buf_offset;
		memmove(sdef->buf, src, left);
		// read in the remainder of the buffer
		dst = sdef->buf + left;
		bytes_needed = BUF_SIZE - left;
	}
	sdef->buf_offset = 0;
	int bytes_left_to_read = bytes_needed;
	do {
		// refill the input if we need to
		if (sdef->zstrm.avail_in == 0) {
			// read from the file
			sdef->zstrm.next_in = sdef->zbuf;
			sdef->zstrm.avail_in = fread(sdef->zbuf, sizeof(uint8_t), BUF_SIZE, sdef->f);
			if (sdef->zstrm.avail_in == 0) {
				printf("we hit the end of the file\n");
				// TODO: we hit the end of the file. do something smart
			}
		}
		sdef->zstrm.avail_out = bytes_left_to_read;
		sdef->zstrm.next_out = dst;
		int ret = inflate(&sdef->zstrm, Z_NO_FLUSH);
		if (ret < 0) {
			printf("\ninflate returned %d\n", ret);
			assert(ret >= 0);
		}
		int have = bytes_left_to_read - sdef->zstrm.avail_out;
		dst += have;
		bytes_left_to_read -= have;
		if (ret == Z_STREAM_END) {
			// we're at the end of the stream. truncate the buffer and go home
			sdef->buf_size = (bytes_needed - bytes_left_to_read);
			bytes_left_to_read = 0;
		}
	} while (bytes_left_to_read > 0);
}

void sorbet_fill_read_buffer(sorbet_def *sdef) {
	if (sdef->compression == 1) {
		sorbet_fill_read_buffer_compressed(sdef);
	} else {
		sorbet_fill_read_buffer_uncompressed(sdef);
	}
}

void sorbet_read_bytes_raw(sorbet_def *sdef, uint8_t *v, int32_t len) {
	// TODO: test whether the length requested goes past the end of the file
	if ((sdef->buf_offset + len) <= sdef->buf_size) {
		// desired number of bytes already in the buffer. just read it.
		uint8_t *src = sdef->buf + sdef->buf_offset;
		memcpy(v, src, len);
		sdef->buf_offset += len;
	} else if (len < sdef->buf_size) {
		// desired number of bytes goes past the end, but we can read it in one go
		sorbet_fill_read_buffer(sdef);
		memcpy(v, sdef->buf, len);
		sdef->buf_offset += len;
	} else {
		// desired number of bytes is bigger than the buffer
		uint8_t *dst = v;
		int bytes_left = len;
		while (bytes_left > 0) {
			sorbet_fill_read_buffer(sdef);
			int bytes_to_read = (bytes_left >= sdef->buf_size) ? sdef->buf_size : bytes_left;
			memcpy(dst, sdef->buf, sdef->buf_size);
			dst += sdef->buf_size;
			sdef->buf_offset += bytes_to_read;
			bytes_left -= bytes_to_read;
		}
	}
	sdef->read_cnt += len;
}

uint8_t sorbet_read_byte_raw(sorbet_def *sdef) {
	uint8_t v;
	sorbet_read_bytes_raw(sdef, &v, 1);
	return v;
}

int32_t sorbet_read_int_raw(sorbet_def *sdef) {
	union {
		int32_t v;
		uint8_t bytes[4];
	} uv;
	sorbet_read_bytes_raw(sdef, (uint8_t  *)&uv, 4);
	return uv.v;
}

int64_t sorbet_read_long_raw(sorbet_def *sdef) {
	union {
		int64_t v;
		uint8_t bytes[8];
	} uv;
	sorbet_read_bytes_raw(sdef, (uint8_t  *)&uv, 8);
	return uv.v;
}

float32_t sorbet_read_float_raw(sorbet_def *sdef) {
	union {
		float32_t v;
		uint8_t bytes[4];
	} uv;
	sorbet_read_bytes_raw(sdef, (uint8_t  *)&uv, 4);
	return uv.v;
}

float64_t sorbet_read_double_raw(sorbet_def *sdef) {
	union {
		float64_t v;
		uint8_t bytes[8];
	} uv;
	sorbet_read_bytes_raw(sdef, (uint8_t  *)&uv, 8);
	return uv.v;
}

void reader_inc_col(sorbet_def *sdef) {
	sdef->cur_col++;
	if (sdef->cur_col >= sdef->schema.numCols) {
		sdef->cur_col = 0;
		sdef->row_cnt++;
	}
	//sdef->cur_col = (sdef->cur_col + 1) % sdef->schema.numCols;
}

bool sorbet_read_int(sorbet_def *sdef, int32_t *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[INTEGER]) {
		*v = sorbet_read_int_raw(sdef);
	} else if (typ == column_type_null_tag[INTEGER]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_long(sorbet_def *sdef, int64_t *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[LONG]) {
		*v = sorbet_read_long_raw(sdef);
	} else if (typ == column_type_null_tag[LONG]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_float(sorbet_def *sdef, float32_t *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[FLOAT]) {
		*v = sorbet_read_float_raw(sdef);
	} else if (typ == column_type_null_tag[FLOAT]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_double(sorbet_def *sdef, float64_t *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[DOUBLE]) {
		*v = sorbet_read_double_raw(sdef);
	} else if (typ == column_type_null_tag[DOUBLE]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_boolean(sorbet_def *sdef, bool *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[BOOLEAN]) {
		uint8_t bv = sorbet_read_byte_raw(sdef);
		*v = (bv == 0) ? false : true;
	} else if (typ == column_type_null_tag[BOOLEAN]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_string(sorbet_def *sdef, char *v, int32_t *len) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[STRING]) {
		int32_t alen = sorbet_read_int_raw(sdef);
		sorbet_read_bytes_raw(sdef, (uint8_t *)v, alen);
		v[alen] = 0;
		*len = alen;
	} else if (typ == column_type_null_tag[STRING]) {
		ret = false;
		*len = 0;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_binary(sorbet_def *sdef, uint8_t *v, int32_t *len) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[BINARY]) {
		int32_t alen = sorbet_read_int_raw(sdef);
		sorbet_read_bytes_raw(sdef, v, alen);
		*len = alen;
	} else if (typ == column_type_null_tag[BINARY]) {
		ret = false;
		*len = 0;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_date(sorbet_def *sdef, sorbet_date *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[DATE]) {
		int32_t dt = sorbet_read_int_raw(sdef);
		v->y = dt / 10000;
		v->m = (dt - (10000 * v->y)) / 100;
		v->d = (dt - ((10000 * v->y)+(100 * v->m)));
	} else if (typ == column_type_null_tag[DATE]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_datetime(sorbet_def *sdef, int64_t *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[DATETIME]) {
		*v = sorbet_read_long_raw(sdef);
	} else if (typ == column_type_null_tag[DATETIME]) {
		ret = false;
	}
	reader_inc_col(sdef);
	return ret;
}

bool sorbet_read_time(sorbet_def *sdef, sorbet_time *v) {
	bool ret = true;
	uint8_t typ = sorbet_read_byte_raw(sdef);
	if (typ == column_type_tag[TIME]) {
		int32_t dt = sorbet_read_int_raw(sdef);
		v->h = dt / 10000;
		v->m = (dt - (10000 * v->h)) / 100;
		v->s = (dt - ((10000 * v->h)+(100 * v->m)));
	} else if (typ == column_type_null_tag[TIME]) {
		v = NULL;
	}
	reader_inc_col(sdef);
}

col_val *sorbet_read_row(sorbet_def *sdef) {
	for (int i=0; i<sdef->schema.numCols; i++) {
		switch (sdef->schema.cols[i].type) {
			case INTEGER: {
				sorbet_read_int(sdef, &sdef->row[i].intval);
				break;
			}
			case LONG: {
				sorbet_read_long(sdef, &sdef->row[i].longval);
				break;
			}
			case FLOAT: {
				sorbet_read_float(sdef, &sdef->row[i].floatval);
				break;
			}
			case DOUBLE: {
				sorbet_read_double(sdef, &sdef->row[i].doubleval);
				break;
			}
			case BOOLEAN: {
				sorbet_read_boolean(sdef, &sdef->row[i].boolval);
				break;
			}
			case STRING: {
				int len;
				sorbet_read_string(sdef, sdef->row[i].strval, &len);
				break;
			}
			case BINARY: {
				int len;
				sorbet_read_binary(sdef, sdef->row[i].binval, &len);
				break;
			}
			case DATE: {
				sorbet_read_date(sdef, &sdef->row[i].dateval);
				break;
			}
			case DATETIME: {
				sorbet_read_datetime(sdef, &sdef->row[i].datetimeval);
				break;
			}
			case TIME: {
				sorbet_read_time(sdef, &sdef->row[i].timeval);
				break;
			}
		}
	}
	return sdef->row;
}

bool read_header(sorbet_def *sdef) {
	// turn off compression while reading the header
	sdef->compression = 0;
	sdef->read_cnt = 0;
	uint64_t sig = sorbet_read_long_raw(sdef);
	if (sig != SORBET_SIGNATURE) {
		printf("%s is not a valid sorbet file\n", sdef->filename);
		return false;
	}
	uint8_t ver = sorbet_read_byte_raw(sdef);
	if (ver > SORBET_VERSION) {
		printf("file version is %d - this reader handles up to %d\n", ver, SORBET_VERSION);
		return false;
	}
	uint8_t compression = sorbet_read_byte_raw(sdef);
	sdef->n_rows = sorbet_read_long_raw(sdef);
	// TODO: do something about negative rows
	sdef->uc_size = sorbet_read_long_raw(sdef);
	// TODO: do something about 0 or negative size
	sdef->schema.numCols = sorbet_read_int_raw(sdef);
	// TODO: do something about 0 or negative cols
	sdef->schema.cols = (data_column *)malloc(sdef->schema.numCols * sizeof(data_column));
	sdef->cstats = (column_stats *)malloc(sdef->schema.numCols * sizeof(column_stats));
	for (int i=0; i<sdef->schema.numCols; i++) {
		int name_len = sorbet_read_int_raw(sdef);
		char *namebuf = (char *)malloc(name_len * sizeof(uint8_t));
		sorbet_read_bytes_raw(sdef, (uint8_t *)namebuf, name_len);
		sdef->schema.cols[i].name = namebuf;
		sdef->schema.cols[i].type = sorbet_read_byte_raw(sdef);
		sdef->schema.cols[i].valType = sorbet_read_byte_raw(sdef);
		sdef->schema.cols[i].keyType = sorbet_read_byte_raw(sdef);
		sdef->cstats[i].cwidth = sorbet_read_int_raw(sdef);
		sdef->cstats[i].cnulls = sorbet_read_long_raw(sdef);
		if (ver > 2) {
			sdef->cstats[i].cbads = sorbet_read_long_raw(sdef);
		} else {
			sdef->cstats[i].cbads = 0;
		}
	}
	sdef->metadataType = sorbet_read_int_raw(sdef);
	sdef->metadataSize = sorbet_read_int_raw(sdef);
	if (sdef->metadataSize > 0) {
		sdef->metadata = (uint8_t *)malloc(sdef->metadataSize * sizeof(uint8_t));
		sorbet_read_bytes_raw(sdef, sdef->metadata, sdef->metadataSize);
	} else {
		sdef->metadata = NULL;
	}
	// turn compression on if needed
	sdef->compression = compression;
	if (compression == 1) {
		sdef->zstrm.zalloc = Z_NULL;
		sdef->zstrm.zfree = Z_NULL;
		sdef->zstrm.opaque = Z_NULL;
		sdef->zstrm.avail_in = 0;
		sdef->zstrm.next_in = Z_NULL;
		//int ret = inflateInit2(&sdef->zstrm, Z_WINDOW_BITS | GZIP_ENCODING);
		int ret = inflateInit2(&sdef->zstrm, GZIP_ENCODING);
		//int ret = inflateInit(&sdef->zstrm);
		if (ret != Z_OK) {
			printf("ERROR: inflateInit returned %d\n", ret);
		}
	}
	printf("seeking to %ld\n", sdef->read_cnt);
	fseek(sdef->f, sdef->read_cnt, 0);
	sdef->buf_offset = BUF_SIZE;
	sorbet_fill_read_buffer(sdef);
	sdef->row_cnt = 0;
	return true;
}

void sorbet_reader_open(sorbet_def *sdef) {
	sdef->buf_size = BUF_SIZE;
	sdef->f = fopen(sdef->filename, "rb");
	sdef->buf_offset = BUF_SIZE;
	sorbet_fill_read_buffer(sdef);
	read_header(sdef);
	sdef->cur_col = 0;
	sdef->row = (col_val *)malloc(sizeof(col_val) * sdef->schema.numCols);
	for (int i=0; i<sdef->schema.numCols; i++) {
		if (sdef->schema.cols[i].type == STRING) {
			sdef->row[i].strval = (char *)malloc(sdef->cstats[i].cwidth + 1);
		} else if (sdef->schema.cols[i].type == BINARY) {
			sdef->row[i].binval = (uint8_t *)malloc(sdef->cstats[i].cwidth);
		}
	}
}

void sorbet_reader_close(sorbet_def *sdef) {
	fclose(sdef->f);
	for (int i=0; i<sdef->schema.numCols; i++) {
		free(sdef->schema.cols[i].name);
	}
	free(sdef->schema.cols);
	free(sdef->cstats);
	if (sdef->metadata != NULL) {
		free(sdef->metadata);
	}
	for (int i=0; i<sdef->schema.numCols; i++) {
		if (sdef->schema.cols[i].type == STRING) {
			free(sdef->row[i].strval);
		} else if (sdef->schema.cols[i].type == BINARY) {
			free(sdef->row[i].binval);
		}
	}
	free(sdef->row);
}
