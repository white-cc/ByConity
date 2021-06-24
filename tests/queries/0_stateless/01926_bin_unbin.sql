select bin(0);
select bin(1);
select bin(10);
select bin(127);
select bin(255);
select bin('0');
select bin('10');
select bin('测试');
select bin(toFixedString('测试', 10));
select bin(toFloat32(1.2));
select bin(toFloat64(1.2));
select bin(toDecimal32(1.2, 8));
select bin(toDecimal64(1.2, 17));

select unbin('00110000'); -- 0
select unbin('0011000100110000'); -- 10
select unbin('111001101011010110001011111010001010111110010101'); -- 测试
