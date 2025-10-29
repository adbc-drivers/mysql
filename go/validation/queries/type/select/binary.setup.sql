DROP TABLE IF EXISTS test_binary;

CREATE TABLE test_binary (
    idx INTEGER,
    res BLOB
);

INSERT INTO test_binary (idx, res) VALUES (1, 0xe38193e38293e381abe381a1e381afe38081e4b896e7958cefbc81);
INSERT INTO test_binary (idx, res) VALUES (2, 0x00);
INSERT INTO test_binary (idx, res) VALUES (3, 0xdeadbeef);
INSERT INTO test_binary (idx, res) VALUES (4, '');
INSERT INTO test_binary (idx, res) VALUES (5, NULL);
