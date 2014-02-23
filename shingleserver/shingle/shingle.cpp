#define SHINGLE_TEST
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include "html.h"
#include "fprint.h"
#include "shingle.h"

#if defined(XSHINGLE_TEST)
#undef malloc
#undef free
void *mi_malloc_X (size_t n, char *file, int line);
void mi_free_X (void *p, char *file, int line);
#define malloc(_n) mi_malloc_X(_n, __FILE__, __LINE__)
#define free(_p) mi_free_X(_p, __FILE__, __LINE__)
#endif

static unsigned char *stralloc(unsigned char *s) {
	char *p = (char *) malloc(strlen((const char*) s) + 1);
	return ((unsigned char*) (strcpy(p, (const char*) s)));
}

static unsigned char *cat(unsigned char *a[], int start, int count) {
	int i;
	int l = 0;
	unsigned char *p;
	for (i = 0; i != count; i++) {
		l += strlen((const char *) a[i]) + 1;
	}
	p = (unsigned char*) malloc(l + 1);
	p[0] = 0;
	i = start;
	do {
		strcat((char*) p, (char*) a[i]);
		strcat((char*) p, " ");
		i++;
		if (i == count) {
			i = 0;
		}
	} while (i != start);
	return (p);
}

void processword(void *v, unsigned char *w, int len);

struct shingle_s {
	fprint_uint64_t *first; /* array size windowsize-1 */
	unsigned char **first_strings; /* array size windowsize-1 */
	fprint_uint64_t *window; /* array size windowsize */
	unsigned char **win_strings; /* array size windowsize */
	unsigned nminima;
	unsigned windowsize;
	int i;
	int i_mod;
	fprint_data_t fp_word;
	fprint_data_t fp_ss;
	fprint_data_t fp_ms;
	fprint_data_t *fp_shingle; /* array size nminima */
	fprint_t *current; /* array size nminima */
	fprint_t *minima; /* array size nminima */
	unsigned char **min_strings; /* array size nminima */
};

void processword(void *v, unsigned char *w, int len) {
	shingle_t s = (shingle_t) v;
	fprint_t fw = fprint_extend(s->fp_word, fprint_empty(s->fp_word), w, len);
	if (s->i < s->windowsize) {
		s->window[s->i_mod] = fw;
#if defined(SHINGLE_TEST)
		s->win_strings[s->i_mod] = stralloc(w);
#endif
		if (s->i == s->windowsize - 1) {
			int m;
#if defined(SHINGLE_TEST)
			unsigned char *best = cat(s->win_strings, 0, s->windowsize);
#endif
			for (m = 0; m != s->nminima; m++) {
				s->current[m] = fprint_extend_word(s->fp_shingle[m],
						fprint_empty(s->fp_shingle[m]), s->window,
						s->windowsize);
				s->minima[m] = s->current[m];
#if defined(SHINGLE_TEST)
				s->min_strings[m] = stralloc(best);
#endif
			}
#if defined(SHINGLE_TEST)
			free(best);
#endif
		} else {
			s->first[s->i] = fw;
			s->first_strings[s->i] = stralloc(w);
		}
	} else {
		int m;
#if defined(SHINGLE_TEST)
		free(s->win_strings[s->i_mod]);
		s->win_strings[s->i_mod] = stralloc(w);
#endif
		for (m = 0; m != s->nminima; m++) {
			s->current[m] = fprint_slideword(s->fp_shingle[m], s->current[m],
					s->window[s->i_mod], fw);
			if (s->current[m] < s->minima[m]) {
				s->minima[m] = s->current[m];
#if defined(SHINGLE_TEST)
				free(s->min_strings[m]);
				s->min_strings[m] = cat(s->win_strings,
						(s->i_mod + 1) % s->windowsize, s->windowsize);
#endif
			}
		}
		s->window[s->i_mod] = fw;
	}
	s->i++;
	s->i_mod++;
	if (s->i_mod == s->windowsize) {
		s->i_mod = 0;
	}
}

shingle_t shingle_new(unsigned windowsize, unsigned nminima) {
	shingle_t s = (shingle_t) malloc(sizeof(*s));
	int fi = 0;
	int i;
	memset(s, 0, sizeof(*s));
	s->nminima = nminima;
	s->windowsize = windowsize;
	s->first = (fprint_uint64_t *) malloc(
			sizeof(s->first[0]) * (s->windowsize - 1));
	s->first_strings = (unsigned char**) malloc(
			sizeof(s->first_strings[0]) * (s->windowsize - 1));
	memset(s->first_strings, 0,
			sizeof(s->first_strings[0]) * (s->windowsize - 1));
	s->window = (fprint_uint64_t *) malloc(
			sizeof(s->window[0]) * s->windowsize);
	s->win_strings = (unsigned char**) malloc(
			sizeof(s->win_strings[0]) * s->windowsize);
	memset(s->win_strings, 0, sizeof(s->win_strings[0]) * s->windowsize);
	s->fp_shingle = (fprint_data_t*) malloc(
			sizeof(s->fp_shingle[0]) * s->nminima);
	s->current = (fprint_t*) malloc(sizeof(s->current[0]) * s->nminima);
	s->minima = (fprint_t *) malloc(sizeof(s->minima[0]) * s->nminima);
	s->min_strings = (unsigned char **) malloc(
			sizeof(s->min_strings[0]) * s->nminima);
	memset(s->min_strings, 0, sizeof(s->min_strings[0]) * s->nminima);
	s->fp_word = fprint_new(fprint_polys[fi++], 0);
	s->fp_ss = fprint_new(fprint_polys[fi++], 0);
	for (i = 0; i != s->nminima; i++) {
		s->fp_shingle[i] = fprint_new(fprint_polys[fi++], s->windowsize);
	}
	s->fp_ms = fprint_new(fprint_polys[fi++], 0);
	return (s);
}

void zapstrings(shingle_t s) {
	int i;
	for (i = 0; i != s->windowsize - 1; i++) {
		if (s->first_strings[i] != 0) {
			free(s->first_strings[i]);
			s->first_strings[i] = 0;
		}
	}
	for (i = 0; i != s->windowsize; i++) {
		if (s->win_strings[i] != 0) {
			free(s->win_strings[i]);
			s->win_strings[i] = 0;
		}
	}
	for (i = 0; i != s->nminima; i++) {
		if (s->min_strings[i] != 0) {
			free(s->min_strings[i]);
			s->min_strings[i] = 0;
		}
	}
	s->i = 0;
	s->i_mod = 0;
}

void shingle_doc(shingle_t s, unsigned char *fp, fprint_t minima[]) {
	int i;
	zapstrings(s);
	html_parse((int (*)(void *)) fgetc, fp, processword, s);
	if (s->i != 0) {
		for (i = 0; i != s->windowsize - 1; i++) {
			processword(s, s->first_strings[i],
					strlen((const char *) s->first_strings[i]));
		}
	}
	memcpy(minima, s->minima, sizeof(minima[0]) * s->nminima);
}

void shingle_supershingle(shingle_t s, fprint_t minima[],
		fprint_t supershingles[], unsigned nsupershingles) {
	unsigned i;
	int j;
	int nj;
	for (i = 0, j = 0; i != nsupershingles; i++, j = nj) {
		nj = (s->nminima * (i + 1)) / nsupershingles;
		supershingles[i] = fprint_extend_word(s->fp_ss, fprint_empty(s->fp_ss),
				&minima[j], nj - j);
	}
}

void shingle_destroy(shingle_t s) {
	int i;
	zapstrings(s);
	for (i = 0; i != s->nminima; i++) {
		fprint_close(s->fp_shingle[i]);
	}
	free(s->current);
	free(s->minima);
	free(s->min_strings);
	fprint_close(s->fp_word);
	fprint_close(s->fp_ss);
	fprint_close(s->fp_ms);
	free(s->fp_shingle);
	free(s->first);
	free(s->first_strings);
	free(s->window);
	free(s->win_strings);
	free(s);
}

#if defined(SHINGLE_TEST)
#include <stdio.h>
#include <ctype.h>
#include <sys/file.h>
#if defined(__CYGWIN__)
#include <io.h>
#endif

void shingle(char *output, unsigned char *input, unsigned ws, unsigned nn) {
	int i;
	unsigned char *ifp;
	char *opt;
	shingle_t s;
	unsigned windowsize = ws;
	unsigned nminima = nn;
	unsigned nsupershingles = 6;
	fprint_t *minima; /* array size nminima */
	fprint_t *supershingles; /* array size nsupershingles */
	fprint_t *megashingles; /* array size nmegashingles */

#if defined(xx__CYGWIN__)
	setmode (0, O_BINARY);
	setmode (1, O_BINARY);
#endif

	ifp = input;
	opt = output;

	minima = (fprint_t *) malloc(sizeof(minima[0]) * nminima);
	supershingles = (fprint_t*) malloc(
			sizeof(supershingles[0]) * nsupershingles);
	s = shingle_new(windowsize, nminima);

	shingle_doc(s, ifp, minima);

	shingle_supershingle(s, minima, supershingles, nsupershingles);
	for (i = 0; i != nsupershingles; i++) {
		std::stringstream ss;
		ss << supershingles[i];
		ss >> opt;
		ss.clear();
		opt += 16;
	}
	output[96] = '\0';
	shingle_destroy(s);

	free(minima);
	free(supershingles);
}

std::vector<fprint_t> yahooShinglesForLongs(unsigned char *input, unsigned ws, unsigned nn) {
	int i;
	unsigned char *ifp;
	shingle_t s;
	unsigned windowsize = ws;
	unsigned nminima = nn;
	unsigned nsupershingles = 6;
	fprint_t *minima; /* array size nminima */
	fprint_t *supershingles; /* array size nsupershingles */
	fprint_t *megashingles; /* array size nmegashingles */

#if defined(xx__CYGWIN__)
	setmode (0, O_BINARY);
	setmode (1, O_BINARY);
#endif

	ifp = input;

	minima = (fprint_t *) malloc(sizeof(minima[0]) * nminima);
	supershingles = (fprint_t*) malloc(
			sizeof(supershingles[0]) * nsupershingles);
	s = shingle_new(windowsize, nminima);
	shingle_doc(s, ifp, minima);
	shingle_supershingle(s, minima, supershingles, nsupershingles);
    std::vector<fprint_t> results;
	for (i = 0; i != nsupershingles; i++) {
		results.push_back(supershingles[i]);
	}
	shingle_destroy(s);

	free(minima);
	free(supershingles);
    return results;
}

std::vector<std::string> yahooShinglesForStrings(unsigned char *input, unsigned ws, unsigned nn) {
	int i;
	unsigned char *ifp;
	shingle_t s;
	unsigned windowsize = ws;
	unsigned nminima = nn;
	unsigned nsupershingles = 6;
	fprint_t *minima; /* array size nminima */
	fprint_t *supershingles; /* array size nsupershingles */
	fprint_t *megashingles; /* array size nmegashingles */

#if defined(xx__CYGWIN__)
	setmode (0, O_BINARY);
	setmode (1, O_BINARY);
#endif

	ifp = input;

	minima = (fprint_t *) malloc(sizeof(minima[0]) * nminima);
	supershingles = (fprint_t*) malloc(
			sizeof(supershingles[0]) * nsupershingles);
	s = shingle_new(windowsize, nminima);

	shingle_doc(s, ifp, minima);
	shingle_supershingle(s, minima, supershingles, nsupershingles);
    std::vector<std::string> results;
	for (i = 0; i != nsupershingles; i++) {
		std::stringstream ss;
        std::string sg;
		ss << supershingles[i];
        ss >> sg;
		ss.clear();
        results.push_back(sg);
	}
	shingle_destroy(s);

	free(minima);
	free(supershingles);
    return results;
}
#endif

