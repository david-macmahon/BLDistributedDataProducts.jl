using BLDistributedDataProducts
using Test

@testset "BLDistributedDataProducts.jl" begin
    @test GBT.fqav(1:4, 4) === 2.5:4.0:2.5
    @test GBT.fqav(1:2:15, 4) === 4.0:8.0:12.0
end
