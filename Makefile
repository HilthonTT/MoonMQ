deps:
	luarocks install busted
	luarocks install luasocket

test:
	busted

.PHONY: deps test