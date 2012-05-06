#!/usr/bin/python2
#
# Processes a flac file into some RDF
# Usage: <script> [rdf filename] [processing type] flac-filename(s)
# processing types are simple, flat, and deep

from __future__ import print_function

import sys
from subprocess import check_output

from rdflib.graph import Graph
from rdflib.term import URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF

import musicbrainzngs as mb
mb.set_useragent("ianmcorvidae's rdfgen.py", "0.0.1", "http://ianmcorvidae.net")

MS = Namespace("http://ianmcorvidae.net/ms-ns/")
GMM = Namespace("http://ianmcorvidae.net/gmm-ns/")

simple_tags = ['artist', 'album', 'title']
flat_tags          = ['artist', 'album', 'title', 'albumartist',  
                      'date', 'script', 'language', 'label', 
                      'totaltracks',  'totaldiscs',  'discnumber', 
                      'format', 'catalognumber', 'releasecountry', 
                      'media', 'asin', 'releasestatus', 'originaldate', 
                      'tracknumber', 'releasetype', 
                      'musicbrainz_albumartistid', 'musicbrainz_albumid', 
                      'musicbrainz_artistid', 'musicbrainz_trackid', 
                      'lyricist', 'composer', 'mixer', 'performer']
name_map = {'releasecountry': 'country', 
            'releasestatus': 'status', 
            'releasetype': 'type'}
flat_tag_nicenames = ['Artist', 'Album', 'Title', 'Album Artist', 
                      'Date', 'Script', 'Language', 'Label', 
                      'Total Tracks', 'Total Discs', 'Disc Number', 
                      'Format', 'Catalog Number', 'Country', 
                      'Media', 'ASIN', 'Status', 'Original Date', 
                      'Track Number', 'Type', 
                      'MusicBrainz Album Artist ID', 'MusicBrainz Album ID', 
                      'MusicBrainz Artist ID', 'MusicBrainz Track ID', 
                      'Lyricist', 'Composer', 'Mixer', 'Performer']

def add_all(store, subject, predicate, tags, tag_name):
    for entry in tags[tag_name]:
        store.add((subject, predicate, Literal(entry)))

# processors
def process_common(store, filename, tags):
    def get_tag(tags, tag, after='_', default=''):
        try:
            return tags[tag][0] + after
        except:
            return default + after

    metaobj = BNode()
    store.add((metaobj, GMM['fileContent'], URIRef('file://' + filename)))
    store.add((metaobj, RDF.type, MS['Track']))

    fname = (get_tag(tags, 'artist') + get_tag(tags, 'album') + 
             get_tag(tags, 'discnumber', '-', '1') + 
             get_tag(tags, 'tracknumber').zfill(len(get_tag(tags, 'totaltracks'))) + 
             get_tag(tags, 'title', '') + '.flac').replace('/', '_')
    store.add((URIRef('file://' + filename), GMM['filename'], Literal(fname)))

    store.add((RDF.type, GMM['niceName'], Literal('Type')))

    return metaobj

def process_simple(store, filename, tags):
    metaobj = process_common(store, filename, tags)

    store.add((MS['artist'], GMM['niceName'], Literal('Artist')))
    store.add((MS['album'], GMM['niceName'], Literal('Album')))
    store.add((MS['title'], GMM['niceName'], Literal('Title')))

    for each in simple_tags:
        add_all(store, metaobj, MS[each], tags, each)

def process_flat(store, filename, tags):
    metaobj = process_common(store, filename, tags)

    for each in flat_tags:
        nicename = Literal(flat_tag_nicenames[flat_tags.index(each)])
        pred = MS[name_map.get(each, each)]
        store.add((pred, GMM['niceName'], nicename))
        try:
            add_all(store, metaobj, pred, tags, each)
        except KeyError:
            pass

def process_deep(store, filename, tags):
    metaobj = process_common(store, filename, tags)
    
    for each in flat_tags:
        nicename = Literal(flat_tag_nicenames[flat_tags.index(each)])
        pred = MS[name_map.get(each, each)]
        store.add((pred, GMM['niceName'], nicename))

    store.add((MS['track'], GMM['niceName'], Literal('Track')))
    store.add((MS['name'], GMM['niceName'], Literal('Name')))
    store.add((MS['sortname'], GMM['niceName'], Literal('Sort Name')))
    store.add((MS['credit'], GMM['niceName'], Literal('Credited Name')))

    # base object stuff
    [add_all(store, metaobj, MS[item], tags, item) for item in 
            ['title', 'tracknumber', 'discnumber']]
            #['lyricist', 'composer', 'mixer', 'performer']

    # album stuff
    album = URIRef('http://musicbrainz.org/release/%s#_' % 
                   tags['musicbrainz_albumid'][0])
    store.add((metaobj, MS['album'], album))
    store.add((album, MS['track'], metaobj))
    store.add((album, RDF.type, MS['Album']))

    [add_all(store, album, MS[name_map.get(item, item)], tags, item) for item in
            ['date', 'originaldate', 'script', 'language', 
             'totaltracks', 'totaldiscs', 'format', 'catalognumber',
             'releasecountry', 'media', 'asin', 
             'releasestatus', 'releasetype']]
             #['label']

    add_all(store, album, MS['title'], tags, 'album')

    # artist stuff
    artists = []
    for each in tags['musicbrainz_artistid']:
        artist = URIRef('http://musicbrainz.org/artist/%s#_' % each)
        store.add((metaobj, MS['artist'], artist))
        store.add((artist, RDF.type, MS['Artist']))
        store.add((artist, MS['track'], metaobj))
        artists.append(artist)

    albumartists = []
    for each in tags['musicbrainz_albumartistid']:
        albumartist = URIRef('http://musicbrainz.org/artist/%s#_' % 
                             tags['musicbrainz_albumartistid'][0])
        store.add((album, MS['artist'], albumartist))
        store.add((albumartist, RDF.type, MS['Artist']))
        store.add((albumartist, MS['album'], album))
        albumartists.append(albumartist)

    for artist in artists:
        add_all(store, artist, MS['credit'], tags, 'artist')
    for albumartist in albumartists:
        add_all(store, albumartist, MS['credit'], tags, 'albumartist')

def postprocess_deep(store, tags):
    artists = dict([(item[-38:-2], item) for item in 
                        store.subjects(RDF.type, MS['Artist'])])
    albums = dict([(item[-38:-2], item) for item in 
                        store.subjects(RDF.type, MS['Album'])])

    for mbid, uri in artists.iteritems():
        try:
            print('API: artist %s' % mbid, file=sys.stderr)
            artist_data = mb.get_artist_by_id(mbid)
            name = artist_data['artist']['name']
            sortname = artist_data['artist']['sort-name']
            store.add((uri, MS['name'], Literal(name)))
            store.add((uri, MS['sortname'], Literal(sortname)))
            if 'country' in artist_data['artist'].keys():
                country = artist_data['artist']['country']
                store.add((uri, MS['country'], Literal(country)))
            if 'type' in artist_data['artist'].keys():
                a_type = artist_data['artist']['type']
                store.add((uri, MS['type'], Literal(a_type)))
        except:
            continue

    for mbid, uri in albums.iteritems():
        try:
            print('API: album %s' % mbid, file=sys.stderr)
            album_data = mb.get_release_by_id(mbid, ['labels'])
            for label_data in album_data['release'].get('label-info-list', []):
                label = URIRef('http://musicbrainz.org/label/%s#_' % 
                               label_data['label']['id']) 
                store.add((uri, MS['label'], label))
                if label not in store.subjects(RDF.type, MS['Label']):
                    print('    label %s' % 
                             label_data['label']['id'], file=sys.stderr)
                    add_label(store, label, label_data)
        except:
            continue

def add_label(store, labeluri, label_data):
    name = label_data['label']['name']
    sortname = label_data['label']['sort-name']
    store.add((labeluri, RDF.type, MS['Label']))
    store.add((labeluri, MS['name'], Literal(name)))
    store.add((labeluri, MS['sortname'], Literal(sortname)))

def setupGraph():
    # setup graph
    store = Graph()
    store.bind('ms', MS)
    store.bind('gmm', GMM)

    return store

if __name__ == '__main__':
    # options
    rdf_filename = sys.argv[1]
    processing_type = sys.argv[2]
    filenames = sys.argv[3:]

    if processing_type != 'all':
        store = setupGraph()
    else:
        stores = [setupGraph() for scheme in ['simple', 'flat', 'deep']]

    for filename in filenames:
        print("Processing %s..." % filename, file=sys.stderr)
        # pull out tags into a dict
        def split_term(term):
            parts = term.split('=')
            return (parts[0], "=".join(parts[1:]).decode('utf-8'))

        metaflac = check_output(['metaflac', '--export-tags-to', '-', filename])
        tags_raw = map(split_term, metaflac.split('\n'))
        tags = {}
        for tag in flat_tags:
            lst = []
            for item in tags_raw:
                if item[0] == tag:
                    lst.append(item[1])
            tags[tag] = lst

        # process
        if processing_type == 'simple':
            process_simple(store, filename, tags)
        elif processing_type == 'flat':
            process_flat(store, filename, tags)
        elif processing_type == 'deep':
            process_deep(store, filename, tags)
        elif processing_type == 'all':
            process_simple(stores[0], filename, tags) 
            process_flat(stores[1], filename, tags) 
            process_deep(stores[2], filename, tags) 
        else:
            raise Exception('unimplemented')


    if processing_type == 'deep' or processing_type == 'all':
        print("Postprocessing for 'deep'...", file=sys.stderr)
        try:
            postprocess_deep(store, tags)
        except:
            postprocess_deep(stores[2], tags)

    # output
    if rdf_filename[-3:] == 'xml':
        ser_format = 'pretty-xml'
    elif rdf_filename[-3:] == 'ntt':
        ser_format = 'nt'
    else:
        ser_format="turtle"
    
    if rdf_filename[0] == '-' and processing_type != 'all':
        print(store.serialize(format=ser_format))
    elif processing_type != 'all':
        store.serialize(rdf_filename, format=ser_format)
    elif processing_type == 'all':
        schemes = ['simple', 'flat', 'deep']
        [stores[n].serialize(rdf_filename % schemes[n], format=ser_format) for 
                n in range(0,3)]

