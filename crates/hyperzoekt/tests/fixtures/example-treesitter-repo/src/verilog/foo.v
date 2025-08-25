module foo();

// simple add function inside module
function automatic int add(input int a, input int b);
    begin
        add = a + b;
    end
endfunction

endmodule
