module simple #(parameter WIDTH = 8) (
    input logic clk,
    input logic rst,
    input logic [WIDTH-1:0] in,
    output logic [WIDTH-1:0] out
);

assign out = in;

endmodule
