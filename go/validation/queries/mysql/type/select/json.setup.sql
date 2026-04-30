DROP TABLE IF EXISTS test_json;

CREATE TABLE test_json (
    idx INTEGER,
    res JSON
);

INSERT INTO test_json (idx, res) VALUES (1, '{"key": "value", "number": 42}');
INSERT INTO test_json (idx, res) VALUES (2, '{"array": [1, 2, 3], "nested": {"inner": true}}');
INSERT INTO test_json (idx, res) VALUES (3, '[]');
INSERT INTO test_json (idx, res) VALUES (4, '{}');
INSERT INTO test_json (idx, res) VALUES (5, 'null');
INSERT INTO test_json (idx, res) VALUES (6, '{"unicode": "测试", "special": "chars@#$%"}');
INSERT INTO test_json (idx, res) VALUES (7, '{"large_number": 9223372036854775807, "boolean": false}');
INSERT INTO test_json (idx, res) VALUES (8, NULL);
