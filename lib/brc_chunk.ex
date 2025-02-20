defmodule Brc.Chunk do
  require Logger
  alias Brc.Chunk.Stats

  @kib 2 ** 10

  def process_file(path, chunksize \\ 256 * @kib) do
    path
    |> stream_file_concurrent([:binary, :read, :raw], chunksize)
    |> Enum.reduce(%{}, fn x, map -> Map.merge(map, x, &Stats.merge_stats/3) end)
    |> Stream.reject(fn {k, _} -> k == "" end)
    # |> Stream.map(&Stats.format_1brc/1)
    |> Stream.map(&Stats.format_test/1)
    |> Enum.sort(fn {a, _}, {b, _} -> a <= b end)
  end

  defp stream_file_concurrent(path, modes, min_chunk_bytes) do
    Stream.resource(
      fn -> File.open!(path, modes) end,
      fn file ->
        case :file.read(file, min_chunk_bytes) do
          :eof ->
            {:halt, file}

          {:ok, chunk} ->
            case :file.read_line(file) do
              :eof -> {[{chunk, "\n"}], file}
              {:ok, rest} -> {[{chunk, rest}], file}
            end
        end
      end,
      &File.close/1
    )
    |> Task.async_stream(&split_lines/1)
    |> Stream.map(fn {:ok, i} -> i end)
  end

  defp split_lines({c, rest}),
    do: :binary.split(c, "\n", [:global]) |> process_lines(rest)

  defp process_lines(lines, rest) do
    lines
    |> Brc.Enum.map_suffix([], rest, &parse_line_binary2/1, &combine_rest/2)
    |> group_by(fn {city, _} -> city end, fn {_, temp} -> temp end)
    |> Enum.into(%{}, fn {city, temps} -> {city, Stats.stats(temps)} end)
  end

  defp group_by(enum, key_fn, val_fn), do: :maps.groups_from_list(key_fn, val_fn, enum)

  defp combine_rest("", "\n"), do: []
  defp combine_rest(last, "\n"), do: [last]
  defp combine_rest("", rest), do: [String.trim_trailing(rest)]
  defp combine_rest(last, <<"\n", rest::binary>>), do: [last, String.trim_trailing(rest)]
  defp combine_rest(last, rest), do: [String.trim_trailing(<<last::binary, rest::binary>>)]

  defp parse_line_binary(line) do
    [city, temp_bin] = :binary.split(line, <<";">>)
    {String.to_atom(city), :erlang.binary_to_float(temp_bin)}
  end

  defp parse_line_binary2(line) do
    [city, temp] = :binary.split(line, <<";">>)

    {:erlang.binary_to_atom(city, :utf8),
     :erlang.binary_to_integer(:binary.replace(temp, <<".">>, ""))}
  end

  defp parse_line_binary3(line) do
    [city, temp_bin] = :binary.split(line, <<";">>)
    [t, <<d::unsigned>>] = :binary.split(temp_bin, <<".">>)
    int = :erlang.binary_to_integer(t)
    dec = d - ?0
    temp = if :binary.at(t, 0) == ?-, do: int * 10 - dec, else: int * 10 + dec
    {String.to_atom(city), temp}
  end

  defmodule Stats do
    @neutral {Float.max_finite(), Float.min_finite(), 0, 0}

    def format_test({k, {mn, mx, sum, cnt}}), do: {k, {mn / 10, sum / cnt / 10, mx / 10}}

    def format_1brc({k, {mn, mx, sum, cnt}}), do: "#{k};#{mn};#{Float.round(sum / cnt, 1)};#{mx}"

    def stats(t), do: {:lists.min(t), :lists.max(t), :lists.sum(t), length(t)}

    def stats2(temps), do: Enum.reduce(temps, @neutral, &stats_reducer/2)

    def stats_reducer(v, {mn, mx, sum, cnt}), do: {min(v, mn), max(v, mx), sum + v, cnt + 1}

    def merge_stats(_k, {min1, max1, total1, len1}, {min2, max2, total2, len2}) do
      {min(min1, min2), max(max1, max2), total1 + total2, len1 + len2}
    end
  end
end
