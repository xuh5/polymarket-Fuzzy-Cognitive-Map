from pmdata.ingest.collect_comments import fingerprint

COMMENT = {"id": "1", "body": "hi", "updatedAt": "t1", "reactionCount": 0, "reportCount": 0,
           "profile": {"name": "a"}}


def test_same_comment_same_fingerprint():
    assert fingerprint(COMMENT) == fingerprint(dict(COMMENT))


def test_edit_or_new_reaction_changes_fingerprint():
    assert fingerprint(COMMENT) != fingerprint({**COMMENT, "body": "hi!", "updatedAt": "t2"})
    assert fingerprint(COMMENT) != fingerprint({**COMMENT, "reactionCount": 1})


def test_profile_changes_do_not_create_new_version():
    assert fingerprint(COMMENT) == fingerprint({**COMMENT, "profile": {"name": "b"}})
