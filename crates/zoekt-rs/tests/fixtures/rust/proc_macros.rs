extern crate proc_macro;
use proc_macro::TokenStream;

// attribute-style proc-macro (declaration only for parsing)
#[proc_macro_attribute]
pub fn attr_macro(_args: TokenStream, input: TokenStream) -> TokenStream {
    input
}

// function-like proc-macro
#[proc_macro]
pub fn derive_something(input: TokenStream) -> TokenStream {
    input
}
