from semanticscholar import SemanticScholar
from refextract import RefExtract
from zotapi import ZotApi
from fuzzywuzzy import fuzz
import subprocess
import re
import sys
import json
import os
import shlex
import logging
import argparse
FORMAT = "%(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

FUZZ_THRESH = 80
refkeyMark0="RK"
refkeyMark1="KR"

def getSMItemID(sm, title):
    if title is None:
        return None
    smitems = sm.searchTitle(id=title)
    for smitem in smitems['data']:
        ratio = fuzz.token_sort_ratio(title, smitem['title'])
        logging.info("RATIO: %d" % ratio)
        logging.info(json.dumps(smitem, indent=2))
        if ratio < FUZZ_THRESH:
            continue
        return smitem['paperId']
    return None

#P_REFS_TEST2 = re.compile(" \[(((\s?\w+\.?\s?)+\d{4}\w?);?)+\]")
def splitrefDash(refstr):
    if '-' not in refstr:
        ref_id = int(refstr)
        # hacky heu
        if ref_id > 500:
            logging.error("Likely Invalid Reference String: %s" % refstr)
            return None
        return [ref_id]
    ref_parts = refstr.split('-')
    if len(ref_parts) != 2:
        logging.error("Invalid Reference String: %s" % refstr)
        return None
    ref_id0 = int(ref_parts[0])
    ref_id1 = int(ref_parts[1])
    # hacky heu
    if ref_id1 - ref_id0 <= 0 or ref_id1 - ref_id0 > 10:
        logging.error("Likely Invalid Reference String: %s" % refstr)
        return None
    return range(ref_id0, ref_id1)

def splitrefSemicolon(refstr):
    if ";" not in refstr:
        return [refstr]
    ref_parts = refstr.split(";")
    ref_ret = []
    for ref in ref_parts:
        ref_ret.extend([ref])
    return ref_ret

def splitrefKomma(refstr):
    if "," not in refstr:
        return splitrefDash(refstr)
    ref_parts = refstr.split(",")
    ref_ret = []
    for ref in ref_parts:
        ref_ret.extend(splitrefDash(ref))
    return ref_ret

P_REFS_TEST2 = re.compile("\[((\w+\.?[\s|,|and|, and]?[\s|\.])+\d{4};?\s?)+\]")
def getAnnotRefKeys2(annot):
    refkeys = set()
    refkeyMarks = set()
    refkeyExpand = {}
    refs_replace = {}
    annot = " ".join(annot.splitlines())
    refs = P_REFS_TEST2.findall(annot)
    for ref_group in refs:
        refstr0 = ref_group[0]
        refstr = ref_group[0]
        logging.info("Found Reference: %s" % refstr)
        ref_replace = "%s" % refstr
        refstr = refstr.replace("[", "")
        refstr = refstr.replace("]", "")
        refstr_split = []
        try:
            refstr_split = list(map(lambda t: t.lstrip().rstrip(), splitrefSemicolon(refstr)))
            logging.debug(refstr_split)
            logging.info("Extracted References: %s" % ", ".join(map(lambda t: "%s" % t, refstr_split)))
            for ref in refstr_split:
                refkeys.add(ref)
                refkeyMarks.add("%s_%s_%s" % (refkeyMark0, ref, refkeyMark1))
            refkeyExpand[refstr0] = ", ".join(map(lambda t: "%s_%s_%s" % (refkeyMark0, t, refkeyMark1), refstr_split))
            logging.info("Expand Reference String: %s -> %s" % (refstr0, refkeyExpand[refstr0]))
        except Exception as e:
            logging.error("Could not parse ref: '%s'" % refstr)
            logging.error(e)
    return refkeys, refkeyMarks, refkeyExpand


P_REFS_TEST = re.compile("(\[(([1-9][0-9]{0,2}\s?[,-]?\s?)+)\])")
def getAnnotRefKeys(annot):
    refkeys = set()
    refkeyMarks = set()
    refkeyExpand = {}
    refs_replace = {}
    annot = " ".join(annot.splitlines())
    annot = annot.replace('–', '-')
    refs = P_REFS_TEST.findall(annot)
    for ref_group in refs:
        refstr0 = ref_group[0]
        refstr = ref_group[0]
        logging.info("Found Reference: %s" % refstr)
        ref_replace = "%s" % refstr
        refstr = refstr.replace("[", "")
        refstr = refstr.replace("]", "")
        refstr = refstr.replace(" ", "")
        refstr_split = []
        try:
            refstr_split = splitrefKomma(refstr)
            logging.info("Extracted References: %s" % ", ".join(map(lambda t: "%d" % t, refstr_split)))
            for ref in refstr_split:
                refkeys.add(ref)
                refkeyMarks.add("%s_%d_%s" % (refkeyMark0, ref, refkeyMark1))
            refkeyExpand[refstr0] = ", ".join(map(lambda t: "%s_%d_%s" % (refkeyMark0, t, refkeyMark1), refstr_split))
            logging.info("Expand Reference String: %s -> %s" % (refstr0, refkeyExpand[refstr0]))
        except Exception as e:
            logging.error("Could not parse ref: '%s'" % refstr)
            logging.error(e)
    return refkeys, refkeyMarks, refkeyExpand

parser = argparse.ArgumentParser(description='Update Annot Refs')
parser.add_argument('-t','--title', help='Paper Title', required=True)
#parser.add_argument('-ck','--ckey', help='Better Bibtex Citation Key', required=False)
parser.add_argument('-a','--annot', help='Annotation File', required=True)
parser.add_argument('-o','--output', help='Output file', required=True)
parser.add_argument('-f','--format', help='Citation Format (1: normal, 2: text)', choices=["1", "2"])
parser.add_argument('--apikey', help='Zotero api key', required=True)
parser.add_argument('--libid', help='Zotero library id', required=True)
parser.add_argument('--libtype', help='Zotero library type', default="user")
parser.add_argument('--libcsv', help='Exported Zotero library (csv format)', default="./My Library.csv")
parser.add_argument('--scache', help='Directory to cache semantic scholar data (by paperid)', default="./smcache")
parser.add_argument('--ccache', help='Directory to cache crossref data (by citation)', default="./cache_by_cite")
parser.add_argument('--tcache', help='Directory to cache various data (by title)', default="./cache_by_title")
parser.add_argument('--anystyle', help='Path to anystyle', default="/home/file/.local/share/gem/ruby/3.0.0/bin/anystyle")
args = parser.parse_args()

#P_URL = re.compile('(https?):\/\/(www\.)?[a-z0-9\.:].*(?=\b)')
#P_URL = re.compile('((https?):\/\/(www\.)?[a-z0-9\.:].*(\s|$))')
#test = "Lucian Cojocar. Commit ﬁxing the memset bug in uClibc-ng, 2016.http://bit.ly/2cx2Lp2"
#m = P_URL.findall(test)
#print(test)
#print(m)
#sys.exit(1)

za = ZotApi(args.libcsv, args.libid, args.libtype, args.apikey)
#TODO
#print(args.ckey)
#if(args.ckey):
#    item = za.getItemByCKey(args.ckey)
#    print("XXXXXXXXXXXXXXX")
#    print(item)
#sys.exit(1)
baseZaKeys = za.getItemIdByFuzzyTitle(args.title)
if len(baseZaKeys) == 0:
    logging.error("Can not find title '%s'" % args.title)
    sys.exit(1)
if len(baseZaKeys) > 1:
    logging.error("Multiple items for title '%s'" % args.title)
    sys.exit(1)
baseZaKey = baseZaKeys[0]
pdfpath = za.getPdfPath(baseZaKey)
logging.info("PDF : %s" % pdfpath)
annot = open(args.annot, "r").read()
if args.format == "2":
    refkeys, refkeyMarks, refkeyExpand = getAnnotRefKeys2(annot)
else:
    refkeys, refkeyMarks, refkeyExpand = getAnnotRefKeys(annot)
# expand refs in annotations
for key, replacement in refkeyExpand.items():
    annot = annot.replace(key, replacement)

sm = SemanticScholar(smcache=args.scache)
logging.info("Extracting references")
refex = RefExtract(sm, za, args.anystyle, cache_by_title=args.tcache, cache_by_cite=args.ccache)
refs = refex.extractRefs(pdfpath, baseZaKey, args.title, refkeys)

ref2ckey = {}
ref2alt = {}
ref2url = {}
ref2info = {}
for ref_id, ref in refs.items():
    #print(json.dumps(ref))
    if ref_id not in refkeys:
        logging.error("Unknown refid: %s" % ref_id)
        continue
    logging.info("." * 80)
    logging.info("ID  : %s" % str(ref_id))
    title = ref['title']
    doi = ref['doi']
    url = ref['url']
    ckey = ref['ckey']
    cite = ref['cite']
    logging.info('TITL: "%s"' % title)
    logging.info("DOI : %s" % doi)
    logging.info("URL : %s" % url)
    logging.info("CKEY: %s" % ckey)
    logging.info("CITE: %s" % cite)
    ref2info[ref_id] = ref
    if ckey:
        ref2ckey[ref_id] = "Reading notes/" + ckey + ".md"
    logging.warning("REF2CKEY: %s\t%s" % (str(ref_id), ckey))
    ref2url[ref_id] = url
    ref2alt[ref_id] = "REF_%s:%s" % (str(ref_id), title)

#P_REFS_TEST = re.compile("(\[(.*?)\])")
#refs_replace = {}
#refs = P_REFS_TEST.findall(annot)
nrprinted = set()
for refid in refkeys:
    refMark = "%s_%s_%s" % (refkeyMark0, str(refid), refkeyMark1)
    logging.info("Replacing Reference Mark: %s" % refMark)
    if refid in ref2ckey.keys():
        logging.info("REPLACE %s -> [[%s]]" % (refMark, ref2ckey[refid]))
        annot = annot.replace(refMark, '[[%s]]' % ref2ckey[refid])
    elif refid in ref2url.keys():
        annot = annot.replace(refMark, '[%s](%s)' % (ref2alt[refid], ref2url[refid]))
        if refid not in nrprinted:
            logging.warning("UPDATE  %s -> \n%s" % (str(refid), json.dumps(ref2info[refid], indent=2)))
            nrprinted.add(refid)
    elif refid in ref2info.keys():
        if refid not in nrprinted:
            logging.warning("UPDATE  %s -> \n%s" % (str(refid), json.dumps(ref2info[refid], indent=2)))
            nrprinted.add(refid)
    else:
        logging.error("Missed Reference: %s" % str(refid))
        annot = annot.replace(refMark, '[[TODO]](%s)' % refMark)
        if refid in ref2info.keys():
            logging.warning("Missed  %s -> \n%s" % (str(refid), json.dumps(ref2info[refid], indent=2)))
            nrprinted.add(refid)

with open(args.output, "w") as fd:
    fd.write(annot)
