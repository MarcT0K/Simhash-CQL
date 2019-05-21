#!/usr/bin/env python3

from sklearn.feature_extraction.text import TfidfVectorizer

from simhash import Simhash, SimhashIndex


class TestSimhash(object):

    def test_int_value(self):
        assert Simhash(0).value == 0
        assert Simhash(4390059585430954713).value == 4390059585430954713
        assert Simhash(9223372036854775808).value == 9223372036854775808

    def test_value(self):
        assert Simhash(['aaa', 'bbb']).value == 57087923692560392

    def test_distance(self):
        sh = Simhash('How are you? I AM fine. Thanks. And you?')
        sh2 = Simhash('How old are you ? :-) i am fine. Thanks. And you?')
        assert sh.distance(sh2) > 0

        sh3 = Simhash(sh2)
        assert sh2.distance(sh3) == 0

        assert Simhash('1').distance(Simhash('2')) != 0

    def test_chinese(self):
        self.maxDiff = None

        sh1 = Simhash('你好　世界！　　呼噜。')
        sh2 = Simhash('你好，世界　呼噜')

        sh4 = Simhash('How are you? I Am fine. ablar ablar xyz blar blar blar blar blar blar blar Thanks.')
        sh5 = Simhash('How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar than')
        sh6 = Simhash('How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar thank')

        assert sh1.distance(sh2) == 0

        assert sh4.distance(sh6) < 3
        assert sh5.distance(sh6) < 3

    def test_short(self):
        shs = [Simhash(s).value for s in ('aa', 'aaa', 'aaaa', 'aaaab', 'aaaaabb', 'aaaaabbb')]

        for i, sh1 in enumerate(shs):
            for j, sh2 in enumerate(shs):
                if i != j:
                    assert sh1 != sh2

    def test_sparse_features(self):
        data = [
            'How are you? I Am fine. blar blar blar blar blar Thanks.',
            'How are you i am fine. blar blar blar blar blar than',
            'This is simhash test.',
            'How are you i am fine. blar blar blar blar blar thank1'
        ]
        vec = TfidfVectorizer()
        D = vec.fit_transform(data)
        voc = dict((i, w) for w, i in vec.vocabulary_.items())

        # Verify that distance between data[0] and data[1] is < than
        # data[2] and data[3]
        shs = []
        for i in range(D.shape[0]):
            Di = D.getrow(i)
            # features as list of (token, weight) tuples)
            features = zip([voc[j] for j in Di.indices], Di.data)
            shs.append(Simhash(features))
        assert shs[0].distance(shs[1]) != 0
        assert shs[2].distance(shs[3]) != 0
        assert shs[0].distance(shs[1]) < shs[2].distance(shs[3])

        # features as token -> weight dicts
        D0 = D.getrow(0)
        dict_features = dict(zip([voc[j] for j in D0.indices], D0.data))
        assert Simhash(dict_features).value == 17583409636488780916

        # the sparse and non-sparse features should obviously yield
        # different results
        assert Simhash(dict_features).value != Simhash(data[0]).value

    def test_equality_comparison(self):
        a = Simhash('My name is John')
        b = Simhash('My name is John')
        c = Simhash('My name actually is Jane')

        assert a == b  # 'A should equal B'
        assert a != c  # 'A should not equal C'


class TestSimhashIndex():
    data = {
        1: 'How are you? I Am fine. blar blar blar blar blar Thanks.',
        2: 'How are you i am fine. blar blar blar blar blar than',
        3: 'This is simhash test.',
        4: 'How are you i am fine. blar blar blar blar blar thank1',
    }

    def setup_class(self):
        objs = [Simhash(v) for _, v in self.data.items()]
        self.index = SimhashIndex(objs, k=10)

    def test_get_near_dup(self):
        s1 = Simhash('How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar thank')
        dups = self.index.get_near_dups(s1)
        assert len(dups) == 3

        self.index.delete(Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        assert len(dups) == 2

        self.index.delete(Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        assert len(dups) == 2

        self.index.add(Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        assert len(dups) == 3

        self.index.add(Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        assert len(dups) == 3
