import logging
import subprocess
import json
import re
import os
import sys
import urllib
import tempfile
import shutil
import hashlib
from bs4 import BeautifulSoup
from datetime import timedelta
from ratelimit import limits, sleep_and_retry
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from crossref.restful import Works, Etiquette
from pdfminer.high_level import extract_text
import tenacity

class RefExtract:
    MIN_TITLE_LEN = 16
    MIN_TKS_RATIO = 80
    MIN_RATIO = 80

    def __init__(self, sm, za, anystyle, cache_by_title="./cache_by_title", cache_by_cite="./cache_by_cite", refstart="References", refstop="Appendix"):
        self.sm = sm
        self.za = za
        self.anystyle = anystyle
        self.refstart = refstart
        self.refstop = refstop
        etq = Etiquette('Replace citations with zotero betterbibtex keys', 'v0.1', 'no url just testing', 'felicitashetzelt@gmail.com')
        self.cache_by_title = cache_by_title
        if not os.path.isdir(self.cache_by_title):
            os.mkdir(self.cache_by_title)
        self.cache_by_cite = cache_by_cite
        if not os.path.isdir(self.cache_by_cite):
            os.mkdir(self.cache_by_cite)
        self.works = Works(etq)

    def __emptyRef(self):
        return {
                    'title': None,
                    'doi': None,
                    'url': None,
                    'ckey': None,
                    }


    def __makeCiteFn(self, cite):
        cite = cite.lower()
        cite = re.sub('[\W_]+', '', cite)
        return os.path.join(self.cache_by_cite, hashlib.md5(cite.encode()).hexdigest())

    def __updateCachedCite(self, cite, key, data):
        '''
        keys are: crossref, smitem
        '''
        if not data:
            return
        fn = self.__makeCiteFn(cite)
        logging.debug("Updating cite cache: %s" % fn)
        old_data = {
                'crossref': {},
                'smitem': {},
                }
        if os.path.exists(fn):
            try:
                old_data = json.loads(open(fn, "r").read())
            except:
                pass
        old_data[key] = data
        open(fn, "w").write(json.dumps(old_data, indent=2))

    def __readCachedCite(self, cite, key):
        '''
        keys are: crossref
        '''
        fn = self.__makeCiteFn(cite)
        try:
            logging.debug("Reading cached data for %s from %s" % (key, fn))
            return json.loads(open(fn, "r").read())[key]
        except:
            pass
        return {}

    def __makeTitleFn(self, title):
        title = title.lower()
        title = re.sub('[\W_]+', '', title)
        return os.path.join(self.cache_by_title, title)

    def __updateCachedTitle(self, title, key, data):
        '''
        keys are: zaitem, smitem, critem, ckey
        '''
        if not data:
            return
        fn = self.__makeTitleFn(title)
        logging.debug("Updating title cache: %s" % fn)
        old_data = {
                'zaitem': {},
                'smitem': {},
                'critem': {},
                'ckey': {},
                }
        if os.path.exists(fn):
            try:
                old_data = json.loads(open(fn, "r").read())
            except:
                pass
        old_data[key] = data
        open(fn, "w").write(json.dumps(old_data, indent=2))


    def __readCachedTitle(self, title, key):
        '''
        keys are: zaitem, smitem, critem, ckey
        '''
        fn = self.__makeTitleFn(title)
        try:
            logging.debug("Reading cached data for %s from %s" % (key, fn))
            return json.loads(open(fn, "r").read())[key]
        except:
            pass
        return {}

    def __matchCite(self, title='', cite=''):
        if len(title) < RefExtract.MIN_TITLE_LEN:
            logging.info("Title to short for cite matching: %s" % title)
            return False
        r = fuzz.token_set_ratio(title.lower(), cite.lower())
        logging.debug("Matched cite ratio %d:\n---\n%s\n---\n%s\n---\n" % (r, title, cite))
        if r > RefExtract.MIN_TKS_RATIO:
            return True
        return False

    def __matchTitle(self, t0, t1):
        t0 = t0.lower()
        t0 = re.sub('[\W_]+', '', t0)
        t1 = t1.lower()
        t1 = re.sub('[\W_]+', '', t1)
        r = fuzz.ratio(t0.lower(), t1.lower())
        logging.debug("Matched title ratio %d:\n---\n%s\n---\n%s\n---\n" % (r, t0, t1))
        if r > RefExtract.MIN_RATIO:
            return True
        return False

    @sleep_and_retry
    @limits(calls=1, period=timedelta(seconds=10).total_seconds())
    def __queryCrossRef(self, cite):
        logging.info("Searching Crossref:\n%s\n" % cite)
        data = {"NODATA": "NOCROSSREF"}
        crossrefs = self.works.query(bibliographic=cite)
        for cr_i, cr in enumerate(crossrefs):
            if cr_i > 4:
                logging.info("Max CrossRef tries reached")
                break
            if 'title' in cr.keys():
                title = cr['title'][0]
                if not self.__matchCite(title=title, cite=cite):
                    continue
                data = cr
                break
        return data

    def __findCrossRef(self, cite):
        cr_data = self.__readCachedCite(cite, 'crossref')
        if cr_data:
            logging.debug("Using cached crossref data")
            return cr_data
        data = self.__queryCrossRef(cite)
        self.__updateCachedCite(cite, 'crossref', data)
        return data

    def __makeRefSemanticScholar(self, smitem):
        ref = self.__emptyRef()
        if 'title' in smitem.keys() and smitem['title'] and smitem['title'] != 'null':
            ref['title'] = smitem['title']
        if 'doi' in smitem.keys() and smitem['doi'] and smitem['doi'] != 'null':
            ref['doi'] = "https://doi.org/%s" % smitem['doi']
        if 'url' in smitem.keys() and smitem['url'] and smitem['url'] != 'null':
            ref['url'] = smitem['url']
        return ref

    def __findSemanticScholarCite(self, cite):
        ref = self.__emptyRef()
        smitem = self.__readCachedCite(cite, 'smitem')
        if smitem:
            logging.debug("Using cached semantic scholar data")
            return self.__makeRefSemanticScholar(smitem)
        try:
            sm_data = self.sm.searchTitle(cite)
            smitem = {"NODATA": "NOSEMANTICSCHOLAR"}
            for sm_entry in sm_data["data"]:
                if self.__matchCite(cite=cite, title=sm_entry["title"]):
                    smitem = self.sm.paper(sm_entry['paperId'])
                    break
            #logging.warn("Failed to find Zotero entry for %s" % title)
            self.__updateCachedCite(cite, 'smitem', smitem)
            return self.__makeRefSemanticScholar(smitem)
        except tenacity.RetryError or ConnectionRefusedError:
            logging.warn("Semantic Scholar issues on cite %s" % cite)
        smitem = {"NODATA": "NOSEMANTICSCHOLAR"}
        return self.__makeRefSemanticScholar(smitem)


    def __findSemanticScholar(self, title):
        ref = self.__emptyRef()
        smitem = self.__readCachedTitle(title, 'smitem')
        if smitem:
            logging.debug("Using cached semantic scholar data")
            return self.__makeRefSemanticScholar(smitem)
        try:
            sm_data = self.sm.searchTitle(title)
            smitem = {"NODATA": "NOSEMANTICSCHOLAR"}
            for sm_entry in sm_data["data"]:
                if self.__matchTitle(title, sm_entry["title"]):
                    smitem = self.sm.paper(sm_entry['paperId'])
                    break
            #logging.warn("Failed to find Zotero entry for %s" % title)
            self.__updateCachedTitle(title, 'smitem', smitem)
            return self.__makeRefSemanticScholar(smitem)
        except tenacity.RetryError or ConnectionRefusedError:
            logging.warn("Semantic Scholar issues on title %s" % title)
        smitem = {"NODATA": "NOSEMANTICSCHOLAR"}
        return self.__makeRefSemanticScholar(smitem)

    def __makeRefZotero(self, zaitem):
        ref = self.__emptyRef()
        try:
            ref['ckey'] = self.za.getCiteKey(zaitem['key'])
            ref['title'] = zaitem['data']['title']
        except Exception as e:
            logging.error("Failed to get info for existing zotero item: %s" % str(e))
            logging.error(json.dumps(zaitem, indent=2))
            sys.exit(1)
        try:
            ref['doi'] = "https://doi.org/%s" % zaitem['data']['doi']
        except:
            pass
        try:
            ref['url'] = zaitem['data']['url']
        except:
            pass

        return ref


    def __findZotero(self, title):
        zaitem = self.__readCachedTitle(title, 'zaitem')
        if zaitem:
            logging.debug("Using cached semantic scholar data")
            return self.__makeRefZotero(zaitem)
        try:
            zaitem = self.za.findItem(title=title)[0]
        except:
            logging.warn("Failed to find Zotero entry for %s" % title)
            return self.__emptyRef()
        self.__updateCachedTitle(title, 'zaitem', zaitem)
        return self.__makeRefZotero(zaitem)

    def __searchTitleSmZa(self, title):
        if len(title) < RefExtract.MIN_TITLE_LEN:
            logging.warn("Skipping potentially broken title: \"%s\"" % title)
            return self.__emptyRef()
        ref_za = self.__findZotero(title)
        # just don't even try sm if za is already found
        if ref_za['title']:
            logging.warn("Found ZA: %s" % ref_za['title'])
            return ref_za
        ref_sm = self.__findSemanticScholar(title)

        if ref_sm['title'] and not ref_za['title']:
            ref_za['title'] = ref_sm['title']

        if ref_sm['doi'] and not ref_za['doi']:
            ref_za['doi'] = ref_sm['doi']

        if ref_sm['url'] and not ref_za['url']:
            ref_za['url'] = ref_sm['url']

        if ref_sm['ckey'] and not ref_za['ckey']:
            ref_za['ckey'] = ref_sm['ckey']

        return ref_za

    def create_temporary_copy(self, path):
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, 'zotgraph_tmp.pdf')
        shutil.copy2(path, temp_path)
        return temp_path

    def __getRefsAnystlye(self, pdfpath):
        cmd = '%s -f json find --no-layout %s -' % (self.anystyle, pdfpath)
        logging.debug("Executing: %s" % cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out = p.communicate()[0]
        refs = json.loads(out.decode('utf-8'))
        return refs

    def __getRefsCrossRef(self, zakey):
        # TODO
        logging.debug("Searching Crossref for target")
        apa_html = self.za.getApa(zaKey)[0]
        apa = BeautifulSoup(apa_html, features="lxml").get_text()
        cr = self.__findCrossRef(apa)
        cr = self.works.query(bibliographic=apa)
        for xxx in cr:
            print(json.dumps(xxx, indent=2))

    def __refTextToKey(self, ref):
        ref = ref.replace(", and", ", ")
        ref = ref.replace("and", ",")
        ref = ref.replace(", ", ",")
        parts = ref.split('.')
        logging.debug("Parse ref: %s" % ref)
        year = parts[-2].lstrip().rstrip()
        logging.debug("Parse year: %s" % year)
        parts_auth = '.'.join(parts[:-2]).split(',')
        logging.debug(parts_auth)
        try:
            if len(parts_auth) == 1:
                return parts_auth[0].lstrip().rstrip().split(' ')[-1] + " " + year
            if len(parts_auth) == 2:
                return " and ".join(map(lambda t: t.lstrip().rstrip().split(' ')[-1], parts_auth)) + " " + year
            return parts_auth[0].lstrip().rstrip().split(' ')[-1] + " et al. " + year
        except:
            return "BrokenRef"

    def __getRefsText2(self, pdfpath, zakey, title, refkeys):
        P_MATCH_CITE2 = re.compile("(^(\w+\.?[\s|,|and|, and]?[\s|\.])+\s\d{4}\.)")
        logging.getLogger("pdfminer").setLevel(logging.WARNING)
        text = extract_text(pdfpath)
        lines = text.split('\n')
        started = False
        rid = None
        refs = {}
        ref = []
        i = 0
        while i < len(lines):
            line = lines[i].rstrip()
            i+=1
            if self.refstop.lower() in line.lower():
                break
            if started:
                m = P_MATCH_CITE2.findall(line)
                if m:
                    logging.debug(m)
                    if len(ref) > 0:
                        if rid in refkeys:
                            refs[rid] = " ".join(ref).replace("[%s]" % str(rid), "").replace("  ", " ").lstrip().rstrip().rstrip('.')
                        ref = []
                    #rid = int(m[0][1])
                    rid = self.__refTextToKey(m[0][0])
                    logging.debug("Found refid: %s", rid)
                if rid is not None:
                    ref.append(line.rstrip('-'))
            if line.lower().startswith(self.refstart.lower()):
                started = True
        if len(ref) > 0 and rid in refkeys:
            refs[rid] = " ".join(ref).replace("[%s]" % str(rid), "").replace("  ", " ").lstrip().rstrip().rstrip('.')

        P_URL = re.compile('((https?):\/\/(www\.)?[a-z0-9\.:].*(\s|$))')
        parsed_refs = {}
        for rid, cite in refs.items():
            logging.debug("Text ref %s:\n%s\n" % (rid, cite))
            parsed_refs[rid] = self.__emptyRef()
            m = P_URL.findall(cite)
            if m:
                logging.info("Detected online ref: %s" % m[0][0])
                parsed_refs[rid]['url'] = m[0][0]


            logging.info("Found title: %s" % title)
            cr = self.__findCrossRef(cite)
            title = None
            try:
                title = cr['title'][0]
                if self.__matchCite(title=title, cite=cite):
                    logging.info("Matched cite title: %s" % (title))
                    parsed_refs[rid] = self.__searchTitleSmZa(title)
            except:
                pass

            try:
                parsed_refs[rid]['doi'] = "https://doi.org/%s" % cr['DOI']
            except:
                pass

            try:
                for link in cr['link']:
                    parsed_refs[rid]['url'] = cr['url']
            except:
                pass

            if not title:
                logging.info("Could not find crossref entry, try scholar:\n%s\n" % cite)
                ref_sm = self.__findSemanticScholarCite(cite)
                parsed_refs[rid]['title'] = ref_sm['title']
                parsed_refs[rid]['doi'] = ref_sm['doi']


        return parsed_refs



    def __getRefsText(self, pdfpath, zakey, title, refkeys):
        P_MATCH_CITE = re.compile("(^\W*\[(\d+)\])")
        logging.getLogger("pdfminer").setLevel(logging.WARNING)
        text = extract_text(pdfpath)
        lines = text.split('\n')
        started = False
        rid = None
        refs = {}
        ref = []
        i = 0
        while i < len(lines):
            line = lines[i].rstrip()
            i+=1
            if self.refstop.lower() in line.lower():
                break
            if started:
                m = P_MATCH_CITE.findall(line)
                if m:
                    logging.debug(m)
                    if len(ref) > 0:
                        if rid in refkeys:
                            refs[rid] = " ".join(ref).replace("[%s]" % str(rid), "").replace("  ", " ").lstrip().rstrip().rstrip('.')
                        ref = []
                    rid = int(m[0][1])
                    logging.debug("Found refid: %s", rid)
                if rid is not None:
                    ref.append(line.rstrip('-'))
            if line.lower().startswith(self.refstart.lower()):
                started = True
        if len(ref) > 0 and rid in refkeys:
            refs[rid] = " ".join(ref).replace("[%s]" % str(rid), "").replace("  ", " ").lstrip().rstrip().rstrip('.')

        P_URL = re.compile('((https?):\/\/(www\.)?[a-z0-9\.:].*(\s|$))')
        parsed_refs = {}
        for rid, cite in refs.items():
            logging.debug("Text ref %s:\n%s\n" % (rid, cite))
            parsed_refs[rid] = self.__emptyRef()
            m = P_URL.findall(cite)
            if m:
                logging.info("Detected online ref: %s" % m[0][0])
                parsed_refs[rid]['url'] = m[0][0]


            logging.info("Found title: %s" % title)
            cr = self.__findCrossRef(cite)
            title = None
            try:
                title = cr['title'][0]
                if self.__matchCite(title=title, cite=cite):
                    logging.info("Matched cite title: %s" % (title))
                    parsed_refs[rid] = self.__searchTitleSmZa(title)
            except:
                pass

            try:
                parsed_refs[rid]['doi'] = "https://doi.org/%s" % cr['DOI']
            except:
                pass

            try:
                for link in cr['link']:
                    parsed_refs[rid]['url'] = cr['url']
            except:
                pass

            if not title:
                logging.info("Could not find crossref entry, try scholar:\n%s\n" % cite)
                ref_sm = self.__findSemanticScholarCite(cite)
                parsed_refs[rid]['title'] = ref_sm['title']
                parsed_refs[rid]['doi'] = ref_sm['doi']


        return parsed_refs

    def __getRefsAnytype(self, pdfpath, zakey, title, refkeys):
        cmd = '%s -f json find --no-layout %s -' % (self.anystyle, pdfpath)
        logging.debug("Executing: %s" % cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        out = p.communicate()[0]
        refs = json.loads(out.decode('utf-8'))

        parsed_refs = {}
        for r in refs:
            try:
                rid = int(r['citation-number'][0])
            except Exception as e:
                logging.error("Anyref is broken")
                logging.error(json.dumps(r, indent=2))
                continue
            if rid not in refkeys:
                continue
            logging.debug("Any ref %s:\n%s\n" % (rid, title))
            parsed_refs[rid] = self.__emptyRef()
            if 'url' in r.keys():
                parsed_refs[rid]['url'] = r['url'][0]
            if 'title' in r.keys():
                titles = sorted(r['title'], key=len, reverse=True)
                for title in titles:
                    isok = False
                    parsed_refs[rid] = self.__searchTitleSmZa(title)
                if len(titles) == 1:
                    parsed_refs[rid]['title'] = titles[0]

        return parsed_refs

    def __getRefs(self, pdfpath, zakey, title, refkeys):
        #refs_text = self.__getRefsText(pdfpath, zakey, title, refkeys)
        refs_text = self.__getRefsText2(pdfpath, zakey, title, refkeys)
        refs_any = self.__getRefsAnytype(pdfpath, zakey, title, refkeys)

        parsed_refs = {}
        rids = set(refs_text.keys()).union(set(refs_any.keys()))
        for rid in rids:
            parsed_refs[rid] = self.__emptyRef()
            if rid in refs_text.keys():
                if refs_text[rid]['title']:
                    parsed_refs[rid]['title'] = refs_text[rid]['title']
                if refs_text[rid]['doi']:
                    parsed_refs[rid]['doi'] = refs_text[rid]['doi']
                if refs_text[rid]['url']:
                    parsed_refs[rid]['url'] = refs_text[rid]['url']
                if refs_text[rid]['ckey']:
                    parsed_refs[rid]['ckey'] = refs_text[rid]['ckey']

            if rid in refs_any.keys():
                if refs_any[rid]['title'] and not parsed_refs[rid]['title']:
                    parsed_refs[rid]['title'] = refs_any[rid]['title']
                if refs_any[rid]['doi'] and not parsed_refs[rid]['doi']:
                    parsed_refs[rid]['doi'] = refs_any[rid]['doi']
                if refs_any[rid]['url'] and not parsed_refs[rid]['url']:
                    parsed_refs[rid]['url'] = refs_any[rid]['url']
                if refs_any[rid]['ckey'] and not parsed_refs[rid]['ckey']:
                    parsed_refs[rid]['ckey'] = refs_any[rid]['ckey']

        return parsed_refs


        #refs_text = self.__getRefsCrossRef(pdfpath)
        #sys.exit(1)
        #for ref in refs:
        #    if 'title' not in ref.keys():
        #        print("Broken ref")
        #        print(json.dumps(ref, indent=2))
        #        continue
        #    print(ref['title'])
        #    titles = sorted(ref['title'], key=len, reverse=True)
        #    print('.' * 80)
        #    for title in titles:
        #        cr = self.__findCrossRef(ref)
        #        for xxx in cr:
        #            print(xxx)
        #        print(cr)
        #        print("Title: %s" % title)
        #        smtitle = self.__findTitleSemanticScholar(title)
        #        print("SMTitle: %s" % smtitle)
        #        sys.exit(1)
        #return pout

    def extractRefs(self, pdfpath, zakey, title, refkeys):
        pdfpath_tmp = self.create_temporary_copy(pdfpath)
        #refpath = os.path.join(self.rcache, hashlib.md5(title.lower().encode()).hexdigest())
        #logging.debug("refpath: " + refpath)
        #if os.path.exists(refpath):
        #    logging.debug("Loading ref data from %s" % refpath)
        #    refs = json.loads(open(refpath, "r").read().replace("\\u201d", "\\\""))
        #else:
        refs = self.__getRefs(pdfpath_tmp, zakey, title, refkeys)
        #logging.debug("Storing ref data to %s" % refpath)
        #open(refpath, "w").write(json.dumps(refs, sort_keys=True, indent=2))
        logging.debug("Parse references for '%s'" % pdfpath)
        return refs
