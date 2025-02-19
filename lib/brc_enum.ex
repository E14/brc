defmodule Brc.Enum do
  def map([], r, _), do: r
  def map([h | t], r, f), do: map(t, [f.(h) | r], f)
end
