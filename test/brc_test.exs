defmodule BrcTest do
  use ExUnit.Case
  doctest Brc

  # takes ~ 316.618s
  def "Reference implementation" do
    {r, d} = Measure.measure(fn -> Brc.run_file_buf("_gen/measurements.txt") end)
    assert d > 1
  end

  def "BrcCity implementation" do
    {r, d} = Measure.measure(fn -> BrcCity.run_file_buf("_gen/measurements.txt") end)
    assert d > 1
  end

  test "Brc.Chunk implementation" do
    chunksize = 256 * 2 ** 10
    {r, d} = Measure.measure(fn -> Brc.Chunk.process_file("_gen/measurements.txt", chunksize) end)
    assert d == data()
  end

  def data do
    Ref.data()
  end
end
