defmodule Brc.Enum do
  def map([], r, _), do: r
  def map([h | t], r, f), do: map(t, [f.(h) | r], f)

  @doc """
  map(data, accumulator, suffix, mapper)
  """
  def map_suffix([], r, _, _, _), do: r
  def map_suffix([h], r, s, m, c), do: map(c.(h, s), r, m)
  def map_suffix([h | t], r, s, m, c), do: map_suffix(t, [m.(h) | r], s, m, c)
end
