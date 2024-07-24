import volga_rust # type: ignore

res = volga_rust.sum_as_string(1, 2)
assert res == '3'

r = volga_rust.test_function()
assert r == 'test'