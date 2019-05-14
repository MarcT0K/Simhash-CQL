#!/usr/bin/env python3

# Created by 1e0n in 2013, Upgraded by Baudroie team in 2019
from __future__ import division, unicode_literals

import re
import sys
import hashlib
import logging
import numbers
import collections
from itertools import groupby
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

if sys.version_info[0] >= 3:
    basestring = str
    unicode = str
    long = int
else:
    range = xrange


def _hashfunc(x):
    return int(hashlib.md5(x).hexdigest(), 16)


class Simhash(object):
    def __init__(self, value, f=64, reg=r"[\w\u4e00-\u9fcc]+", hashfunc=None, log=None):
        """
        `f` is the dimensions of fingerprints

        `reg` is meaningful only when `value` is basestring and describes
        what is considered to be a letter inside parsed string. Regexp
        object can also be specified (some attempt to handle any letters
        is to specify reg=re.compile(r'\w', re.UNICODE))

        `hashfunc` accepts a utf-8 encoded string and returns a unsigned
        integer in at least `f` bits.
        """

        self.f = f
        self.reg = reg
        self.value = None

        if hashfunc is None:
            self.hashfunc = _hashfunc
        else:
            self.hashfunc = hashfunc

        if log is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = log

        if isinstance(value, Simhash):
            self.value = value.value
        elif isinstance(value, basestring):
            self.build_by_text(unicode(value))
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        elif isinstance(value, numbers.Integral):
            self.value = value
        else:
            raise Exception("Bad parameter with type {}".format(type(value)))

    def __eq__(self, other):
        """
        Compare two simhashes by their value.

        :param Simhash other: The Simhash object to compare to
        """
        return self.value == other.value

    def _slide(self, content, width=4):
        return [content[i : i + width] for i in range(max(len(content) - width + 1, 1))]

    def _tokenize(self, content):
        content = content.lower()
        content = "".join(re.findall(self.reg, content))
        ans = self._slide(content)
        return ans

    def build_by_text(self, content):
        features = self._tokenize(content)
        features = {k: sum(1 for _ in g) for k, g in groupby(sorted(features))}
        return self.build_by_features(features)

    def build_by_features(self, features):
        """
        `features` might be a list of unweighted tokens (a weight of 1
                   will be assumed), a list of (token, weight) tuples or
                   a token -> weight dict.
        """
        v = [0] * self.f
        masks = [1 << i for i in range(self.f)]
        if isinstance(features, dict):
            features = features.items()
        for f in features:
            if isinstance(f, basestring):
                h = self.hashfunc(f.encode("utf-8"))
                w = 1
            else:
                assert isinstance(f, collections.Iterable)
                h = self.hashfunc(f[0].encode("utf-8"))
                w = f[1]
            for i in range(self.f):
                v[i] += w if h & masks[i] else -w
        ans = 0
        for i in range(self.f):
            if v[i] > 0:
                ans |= masks[i]
        self.value = ans

    def distance(self, another):
        assert self.f == another.f
        x = (self.value ^ another.value) & ((1 << self.f) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x - 1
        return ans


class SimhashIndex(object):
    def __init__(self, objs, f=64, k=2, log=None, cleandb=True):
        """
        `objs` is a list of simhash
        `f` is the same with the one for Simhash
        `k` is the tolerance
        """
        self.k = k
        self.f = f
        count = len(objs)

        if log is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = log

        self.log.info("Initializing %s data.", count)

        cluster = Cluster(["ns305788.ip-91-121-221.eu"])
        self.session = cluster.connect()

        if cleandb:
            self.session.execute("""DROP KEYSPACE IF EXISTS simhash""")
            self.session.execute(
                """CREATE KEYSPACE simhash
                                WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}"""
            )
            self.session.execute("USE simhash")
            for i in range(self.k + 1):
                self.session.execute(
                    "CREATE TABLE hash"
                    + str(i)
                    + "(hash TEXT, hashpart TEXT, PRIMARY KEY(hashpart, hash))"
                )

        self.insert_hash = [
            self.session.prepare(
                "INSERT INTO hash" + str(i) + "(hash,hashpart) VALUES(?,?)"
            )
            for i in range(self.k + 1)
        ]
        self.delete_hash = [
            self.session.prepare(
                "DELETE FROM hash" + str(i) + " WHERE hash = ? AND hashpart = ?"
            )
            for i in range(self.k + 1)
        ]

        for i, q in enumerate(objs):
            if i % 10000 == 0 or i == count - 1:
                self.log.info("%s/%s", i + 1, count)

            self.add(q)

    def get_near_dups(self, simhash):
        """
        `simhash` is an instance of Simhash
        return a list of obj_id, which is in type of str
        """
        assert simhash.f == self.f

        ans = set()

        for i, key in enumerate(self.get_keys(simhash)):
            dups = [
                row[0]
                for row in self.session.execute(
                    "SELECT hash FROM hash%d WHERE hashpart = '%s'" % (i, key)
                )
            ]
            self.log.debug("key:%s", key)
            if len(dups) > 200:
                self.log.warning("Big bucket found. key:%s, len:%s", key, len(dups))

            for dup in dups:
                sim2 = Simhash(long(dup, 16), self.f)

                d = simhash.distance(sim2)
                if d <= self.k:
                    ans.add(sim2.value)
        return ans

    def add(self, simhash):
        """
        `obj_id` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        v = "%x" % (simhash.value)
        batch = BatchStatement()
        for i, key in enumerate(self.get_keys(simhash)):
            batch.add(self.insert_hash[i], (v, key))
        self.session.execute(batch)

    def delete(self, simhash):
        """
        `obj_id` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        v = "%x" % (simhash.value)
        batch = BatchStatement()
        for i, key in enumerate(self.get_keys(simhash)):
            batch.add(self.delete_hash[i], (v, key))
        self.session.execute(batch)

    @property
    def offsets(self):
        """
        You may optimize this method according to <http://www.wwwconference.org/www2007/papers/paper215.pdf>
        """
        return [self.f // (self.k + 1) * i for i in range(self.k + 1)]

    def get_keys(self, simhash):
        for i, offset in enumerate(self.offsets):
            if i == (len(self.offsets) - 1):
                m = 2 ** (self.f - offset) - 1
            else:
                m = 2 ** (self.offsets[i + 1] - offset) - 1
            c = simhash.value >> offset & m
            yield "%x" % c
