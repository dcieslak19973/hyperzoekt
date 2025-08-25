module ex1();
  initial begin
    $display("Verilog uses $fatal for fatal errors");
    $fatal(1);
  end
endmodule
module ex_fatal();
    initial begin
        $display("Verilog: no exceptions; use $fatal for fatal errors");
        $fatal(1);
    end
endmodule
