#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"


MODULE = DBIx::TextIndex		PACKAGE = DBIx::TextIndex

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
    freqs = (HV *)sv_2mortal((SV *)newHV());
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "unterminated compressed integer");
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
    XPUSHs(newRV_inc((SV *)freqs));
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
    results = (AV *)sv_2mortal((SV *)newAV());
    /* last byte cannot have high bit set */
    if (*(string + length) & 0x80)
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "unterminated compressed integer");
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
    XPUSHs(newRV_inc((SV *)results));
}



void
term_docs_and_freqs(packed)
SV *packed
PROTOTYPE: $
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
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "unterminated compressed integer");
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
    if ( ! ( SvROK(ints_arrayref) &&
             (term_freqs = (AV*)SvRV(ints_arrayref)) &&
             SvTYPE(term_freqs) == SVt_PVAV                   )   )
    {
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "args must be arrayref");
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
    AV *term_docs;
    I32 length = 0;
    unsigned int i, j, last_doc, doc, freq, value;
    register unsigned long buff;
    if ( ! ( SvROK(term_docs_arrayref) &&
             (term_docs = (AV*)SvRV(term_docs_arrayref)) &&
             SvTYPE(term_docs) == SVt_PVAV                   )   )
    {
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "args must be arrayref");
    }
    length = av_len(term_docs);
    if (length < 1)
        XSRETURN_UNDEF;
    if ((length + 1) % 2 != 0)
        croak("DBIx::TextIndex::%s(): %s", GvNAME(CvGV(cv)), "array must contain even number of elements");
    New(1,  packed, (4 * (length + 1)), char );
    j = 0;
    last_doc = 0;
    for (i = 0 ; i <= length ; i+= 2) {
        doc  = SvIV(*av_fetch(term_docs, i, 0));
	freq = SvIV(*av_fetch(term_docs, i + 1, 0));

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
    XPUSHs(sv_2mortal(newSVpv(packed, j)));
    Safefree(packed);
}
