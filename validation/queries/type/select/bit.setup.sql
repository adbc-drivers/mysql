DROP TABLE IF EXISTS test_bit;


CREATE TABLE test_bit (
   idx INTEGER,
   res BIT(8)
);


INSERT INTO test_bit (idx, res) VALUES (1, b'10101010');
INSERT INTO test_bit (idx, res) VALUES (2, b'11110000');
INSERT INTO test_bit (idx, res) VALUES (3, b'00001111');
INSERT INTO test_bit (idx, res) VALUES (4, b'11111111');
INSERT INTO test_bit (idx, res) VALUES (5, b'00000000');
INSERT INTO test_bit (idx, res) VALUES (6, b'01010101');
INSERT INTO test_bit (idx, res) VALUES (7, b'10011001');
INSERT INTO test_bit (idx, res) VALUES (8, NULL);