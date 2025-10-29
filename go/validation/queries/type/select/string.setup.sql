DROP TABLE IF EXISTS test_string;

CREATE TABLE test_string (
    idx INTEGER,
    res VARCHAR(1000)
);

INSERT INTO test_string (idx, res) VALUES (1, 'hello');
INSERT INTO test_string (idx, res) VALUES (2, '');
INSERT INTO test_string (idx, res) VALUES (3, 'Special chars: !@#$%^&*()_+{}|:"<>?~`-=[]\\;\',./');
INSERT INTO test_string (idx, res) VALUES (4, 'Unicode: 你好, Привет, こんにちは, สวัสดี');
INSERT INTO test_string (idx, res) VALUES (5, NULL);
