#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#define TEXTINDEX_DEREF_AV(ref,av) ( ref && SvROK(ref) && \
             (av = (AV*)SvRV(ref)) && \
             SvTYPE(av) == SVt_PVAV )

#define TEXTINDEX_DEREF_HV(ref,hv) ( ref && SvROK(ref) && \
             (hv = (HV*)SvRV(ref)) && \
             SvTYPE(hv) == SVt_PVHV ) 

#define TEXTINDEX_DEREF_BITVEC(ref, obj, ptr) ( ref && SvROK(ref) && \
             (obj = (SV*)SvRV(ref)) && \
             SvOBJECT(obj) && \
             SvREADONLY(obj) && \
             (SvTYPE(obj) == SVt_PVMG) && \
             (ptr = (unsigned int *)SvIV(obj)) )

#define TEXTINDEX_ERROR(error) \
    croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), error);

#define BITVEC_TEST_BIT(address,index) \
    ((*(address+(index>>5)) & BITMASKS[index & 31]) != 0)

static unsigned int *BITMASKS;

void bitvec_boot(void);
unsigned int get_doc_freq_pair(char *, unsigned int, unsigned int, unsigned int *, unsigned int *g);
int bitvec_test_bit(unsigned int *, unsigned int);

int bitvec_test_bit(unsigned int *addr, unsigned int index) {
    if (index < *(addr - 3))
        return( BITVEC_TEST_BIT(addr,index) );
    else
        return( 0 );
}

void bitvec_boot(void) {
    int i;
    BITMASKS = (unsigned int *) malloc((size_t) 128); /* Assume 32 bit word */
    for (i = 0; i < 32; i++) {
        BITMASKS[i] = (1 << i);
    }
}

unsigned int get_doc_freq_pair(char *string, unsigned int pos, unsigned int last_doc, unsigned int *doc, unsigned int *freq) {
    unsigned int value = 0;
    char temp;
    int got_freq = 0;
    int freq_is_next = 0;
    while (! got_freq) {
	value = *(string + pos); pos++;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *(string + pos); pos++;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}

	if ( freq_is_next ) {
	    *freq = value;
            got_freq = 1;
	    continue;
        }

	*doc = last_doc + (value >> 1);
	if (value & 1) {
            *freq = 1;
            got_freq = 1;
	} else {
	    freq_is_next = 1;
	}
    }
    return pos;
}

MODULE = DBIx::TextIndex		PACKAGE = DBIx::TextIndex

PROTOTYPES: DISABLE

BOOT:
{
    /* FIXME: error check */
    bitvec_boot();
}

void
term_docs_hashref(packed)
  SV *packed
PPCODE:
{
    HV *freqs;
    char *string;
    STRLEN len;
    int length;
    unsigned int value;
    int freq_is_next = 0;
    unsigned int doc = 0;
    char temp;

    string = SvPV(packed, len);
    length = len;
    freqs = newHV();
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *string++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *string++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}
	if ( freq_is_next ) {
	    hv_store_ent(freqs, newSViv(doc), newSViv(value), 0);
            freq_is_next = 0;
	    continue;
        } 

	doc += value >> 1;
	if (value & 1) {
	    hv_store_ent(freqs, newSViv(doc), newSViv(1), 0);
	} else {
	    freq_is_next = 1;
	}
    }
    XPUSHs(sv_2mortal(newRV_noinc((SV *)freqs)));
}


void
term_docs_arrayref(packed)
  SV *packed
PPCODE:
{
    AV *results;
    char *string;
    STRLEN len;
    int length;
    unsigned int value;
    int freq_is_next = 0;
    unsigned int doc = 0;
    char temp;

    string = SvPV(packed, len);
    length = len;
    results = newAV();
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *string++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *string++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}

	if ( freq_is_next ) {
	    av_push(results, newSViv(value));
            freq_is_next = 0;
	    continue;
        }

	doc += value >> 1;
	   av_push(results, newSViv(doc));
	if (value & 1) {
	    av_push(results, newSViv(1));
	} else {
	    freq_is_next = 1;
	}
    }
    XPUSHs(sv_2mortal(newRV_noinc((SV *)results)));
}

void
term_doc_ids_arrayref(packed)
  SV *packed
PPCODE:
{
    AV *results;
    char *string;
    STRLEN len;
    int length;
    unsigned int value;
    int freq_is_next = 0;
    unsigned int doc = 0;
    char temp;

    string = SvPV(packed, len);
    length = len;
    results = newAV();
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *string++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *string++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}

	if ( freq_is_next ) {
            freq_is_next = 0;
	    continue;
        }

	doc += value >> 1;
	   av_push(results, newSViv(doc));

	if (! (value & 1)) {
	    freq_is_next = 1;
	}
    }
    XPUSHs(sv_2mortal(newRV_noinc((SV *)results)));
}


void
term_docs_array(packed)
  SV *packed
PPCODE:
{
    char *string;
    STRLEN len;
    int length;
    unsigned int value;
    int freq_is_next = 0;
    unsigned int doc = 0;
    char temp;

    string = SvPV(packed, len);
    length = len;
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *string++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *string++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}
	if ( freq_is_next ) {
	    XPUSHs(sv_2mortal(newSViv(value)));
            freq_is_next = 0;
	    continue;
        }

	doc += value >> 1;
	    XPUSHs(sv_2mortal(newSViv(doc)));
	if (value & 1) {
	    XPUSHs(sv_2mortal(newSViv(1)));
	} else {
	    freq_is_next = 1;
	}
    }
}


void
term_docs_and_freqs(packed)
  SV *packed
PPCODE:
{
    AV *docs;
    AV *freqs;
    char *string;
    STRLEN len;
    int length;
    unsigned int value;
    int freq_is_next = 0;
    unsigned int doc = 0;
    char temp;

    string = SvPV(packed, len);
    length = len;
    docs = (AV *)sv_2mortal((SV *)newAV());
    freqs = (AV *)sv_2mortal((SV *)newAV());
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *string++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *string++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}
	if ( freq_is_next ) {
	    av_push(freqs, newSViv(value));
            freq_is_next = 0;
	    continue;
        } 

	doc += value >> 1;
	    av_push(docs, newSViv(doc));
	if (value & 1) {
	    av_push(freqs, newSViv(1));
	} else {
	    freq_is_next = 1;
	}
    }

    XPUSHs(newRV_inc((SV *)docs));
    XPUSHs(newRV_inc((SV *)freqs));
}


void
pack_vint(ints_arrayref)
  SV *ints_arrayref
PPCODE:
{
    char *packed;
    AV *term_freqs;
    I32 length = 0;
    unsigned int i, j, value;
    register unsigned long buff;
    if (! TEXTINDEX_DEREF_AV(ints_arrayref, term_freqs )) {
        TEXTINDEX_ERROR("args must be arrayref");
    }
    length = av_len(term_freqs);
    if (length < 0)
        XSRETURN_UNDEF;
    New(1,  packed, (4 * (length + 1)), char );
    j = 0;
    for (i = 0 ; i <= length ; i++) {
        value = SvIV(*av_fetch(term_freqs, i, 0));
 	buff = value & 0x7f;
	while ((value >>= 7)) {
	    buff <<= 8;
            buff |= ((value & 0x7f) | 0x80);
        }

        while (1) {
            *(packed + j) = buff;
            j++;
            if (buff & 0x80)
                buff >>= 8;
            else
                break;
        }
    }
    XPUSHs(sv_2mortal(newSVpv(packed, j)));
    Safefree(packed);
}


void
pack_term_docs(term_docs_arrayref)
  SV *term_docs_arrayref
PPCODE:
{
    char *packed;
    I32 length = 0;
    unsigned int i, j, last_doc, value;
    register unsigned long buff;
    if (( !SvROK(term_docs_arrayref)
           || (SvTYPE(SvRV(term_docs_arrayref)) != SVt_PVAV) ))
    {
        TEXTINDEX_ERROR("args must be arrayref");
    }
    length = av_len((AV *)SvRV(term_docs_arrayref));
    if (length < 1)
        XSRETURN_UNDEF;
    if ((length + 1) % 2 != 0)
        TEXTINDEX_ERROR("array must contain even number of elements");
    New(1,  packed, (4 * (length + 1)), char);
    if (packed == NULL)
        TEXTINDEX_ERROR("unable to allocate memory");
    j = 0;
    last_doc = 0;
    for (i = 0 ; i <= length ; i+= 2) {
        int doc  = SvIV(*av_fetch((AV *)SvRV(term_docs_arrayref), i, 0));
	int freq = SvIV(*av_fetch((AV *)SvRV(term_docs_arrayref), i + 1, 0));

	value = (doc - last_doc) << 1;
	if (freq == 1)
            value += 1;

        buff = value & 0x7f;
        while ((value >>= 7)) {
	    buff <<= 8;
            buff |= ((value & 0x7f) | 0x80);
        }
        while (1) {
            *(packed + j) = buff;
            j++;
            if (buff & 0x80)
                buff >>= 8;
            else
                break;
        }
        if (freq > 1) {
            buff = freq & 0x7f;
            while ((freq >>= 7)) {
	        buff <<= 8;
                buff |= ((freq & 0x7f) | 0x80);
            }
            while (1) {
                *(packed + j) = buff;
                j++;
                if (buff & 0x80)
                    buff >>= 8;
                else
                    break;
            }
        }
        last_doc = doc;
    }
    XPUSHs(sv_2mortal(newSVpv((char *)packed, j)));
    Safefree(packed);
}

void
pack_term_docs_append_vint(packed, vint)
  SV *packed
  SV *vint
PPCODE:
{
    char *str_a, *str_b, *newpack;
    STRLEN len_a, len_b;
    I32 length_a = 0;
    I32 length_b = 0;
    int length = 0;
    int freq_is_next = 0;
    unsigned int value, val, i, j, freq;
    unsigned int doc = 0;
    unsigned int max_doc = 0;
    unsigned int last_doc = 0;
    register unsigned long buff;
    char temp;

    str_a = SvPV(packed, len_a);
    length_a = len_a;

    str_b = SvPV(vint, len_b);
    length_b = len_b;

    if (length_b < 1) {
        XPUSHs(sv_2mortal(newSVpv((char *)str_a, length_a)));
        return;
    }	

    New(2, newpack, ( length_a + (4 * (length_b + 1)) ), char);
    if (newpack == NULL)
        TEXTINDEX_ERROR("unable to allocate memory");

    Copy(str_a, newpack, length_a, char);

    /* Step 1: get max_doc (highest doc id) from 1st arg (packed) */

    length = length_a;
    /* last byte cannot have high bit set */
    if (*(str_a + length) & 0x80)
        TEXTINDEX_ERROR("unterminated compressed integer");
    while (length > 0) {
	value = *str_a++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *str_a++; length--;
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}
	if ( freq_is_next ) {
            freq_is_next = 0;
	    continue;
        } 

	doc += value >> 1;
            max_doc = doc;

	if (! (value & 1)) {
	    freq_is_next = 1;
	}
    }

    /* Step 2: unpack 2nd arg (vint) and repack as deltas */


    last_doc = max_doc;

    i = 0;
    j = length_a;
    length = length_b;
    while (length > 0) {
	value = *str_b++; length--;
	if (value & 0x80)
	{
	    value &= 0x7f;
	    do
	    {
		temp = *str_b++; length--;
	        if (length < 0)
	            TEXTINDEX_ERROR("unterminated compressed integer"); 
		value = (value << 7) + (temp & 0x7f);
	    } while (temp & 0x80);
	}
 	if (i % 2 == 0) {
            doc = value;
        } else {
            freq = value;

	    val = (doc - last_doc) << 1;
            if (freq == 1)
                val += 1;

            buff = val & 0x7f;
            while ((val >>= 7)) {
	        buff <<= 8;
                buff |= ((val & 0x7f) | 0x80);
            }

            while (1) {
                *(newpack + j) = buff;
                j++;
                if (buff & 0x80)
                    buff >>= 8;
                else
                    break;
            }
            if (freq > 1) {
                buff = freq & 0x7f;
                while ((freq >>= 7)) {
	            buff <<= 8;
                    buff |= ((freq & 0x7f) | 0x80);
                }
                while (1) {
                    *(newpack + j) = buff;
                    j++;
                    if (buff & 0x80)
                        buff >>= 8;
                    else
                        break;
                }
            }
            last_doc = doc;
        }
        i++;
    }
    XPUSHs(sv_2mortal(newSVpv((char *)newpack, j)));
    Safefree(newpack);
}

void
score_term_docs_okapi(term_docs, score_hashref, bitvec_ref, acc_lim_SV, \
                      res_min_SV, res_max_SV, idf_SV, f_t_SV, W_D_arrayref, \
                      avg_W_d_SV, w_qt_SV, k1_SV, b_SV)
  SV *term_docs
  SV *score_hashref
  SV *bitvec_ref
  SV *acc_lim_SV
  SV *res_min_SV
  SV *res_max_SV
  SV *f_t_SV
  SV *idf_SV
  SV *W_D_arrayref
  SV *avg_W_d_SV
  SV *w_qt_SV
  SV *k1_SV
  SV *b_SV
PPCODE:
{
    int          acc_size,
                 length;
    unsigned int acc_lim    =  SvIV(acc_lim_SV),
                 f_t        =  SvIV(f_t_SV),
                 res_min    =  SvIV(res_min_SV), 
                 res_max    =  SvIV(res_max_SV),
                 doc,
                 last_doc,
                 f_dt,
                 old_score,
                 i,
                 pos,
                 *bitvec;

    double       idf        =  SvNV(idf_SV),
                 avg_W_d    =  SvNV(avg_W_d_SV),
                 w_qt       =  SvNV(w_qt_SV),
                 k1         =  SvNV(k1_SV),
                 b          =  SvNV(b_SV),
                 W_d,
                 TF,
                 doc_score;
    char *string;
    SV *bitvec_obj;
    SV *doc_id;
    AV *W_D;
    HV *score;
    HE *score_he;
    STRLEN len;

    string  = SvPV(term_docs, len);
    length  = len;

    if (! TEXTINDEX_DEREF_AV(W_D_arrayref, W_D)) {
        TEXTINDEX_ERROR("arg9 must be arrayref");
    }
    if (! TEXTINDEX_DEREF_HV(score_hashref, score)) {
        TEXTINDEX_ERROR("arg2 must be arrayref");
    }
    if (! TEXTINDEX_DEREF_BITVEC(bitvec_ref, bitvec_obj, bitvec)) {
        TEXTINDEX_ERROR("arg3 must be Bit::Vector object");
    }

    pos = 0;
    last_doc = 0;
    acc_size = 0;
    for (i = 0; (i < f_t) && (acc_size < acc_lim); i++) {
	pos = get_doc_freq_pair(string, pos, last_doc, &doc, &f_dt);
	last_doc = doc;
	if (doc > res_max) break;
        if (doc < res_min) continue;
        if ( ! bitvec_test_bit(bitvec, doc) ) continue;
        W_d = SvNV(*av_fetch(W_D, doc, 0));
        TF = (((k1 + 1) * f_dt) / (k1 * ((1 - b)+((b * W_d)/avg_W_d)) + f_dt));
        doc_score = idf * TF * w_qt;
        doc_id = newSViv(doc);  
        score_he = hv_fetch_ent(score, doc_id, TRUE, 0);
        if (old_score = SvIV(HeVAL(score_he)))
            doc_score += old_score;
        hv_store_ent(score, doc_id, newSVnv(doc_score), 0);
        acc_size = HvKEYS(score);
    }
}
