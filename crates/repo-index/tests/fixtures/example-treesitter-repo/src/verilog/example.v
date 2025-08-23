module foo #(parameter N = 8) ();
    // simple function inside module
    function automatic int add(input int a, input int b);
        add = a + b;
    endfunction
endmodule

module top ();
    // instantiate foo
    foo #(.N(8)) u_foo ();
endmodule
