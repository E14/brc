defmodule Measure do
  def measure(fun) do
    c = :os.perf_counter()
    r = fun.()
    d = :erlang.convert_time_unit(:os.perf_counter() - c, :perf_counter, :millisecond) / 1_000.0
    {r, d}
  end
end