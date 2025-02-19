defmodule Brc.Chunk do
  require Logger
  alias Brc.Chunk.Stats

  @kib 2 ** 10
  @mib 2 ** 20

  def process_file(path) do
    path
    |> stream_file_concurrent([], 512 * @kib)
    |> Enum.reduce(%{}, fn x, map -> Map.merge(map, x, &Stats.merge_stats/3) end)
    |> Stream.reject(fn {k, _} -> k == "" end)
    |> Stream.map(&Stats.format/1)
    |> Enum.sort()
  end

  def process_lines(lines) do
    lines
    |> Brc.Enum.map([], &parse_line_binary/1)
    |> Enum.group_by(fn {city, _} -> city end, fn {_, temp} -> temp end)
    |> Enum.into(%{}, fn {city, temps} -> {city, Stats.stats(temps)} end)
  end

  def process_lines2(lines) do
    lines
    |> Brc.Enum.map([], &parse_line_binary/1)
    |> group_by(fn {city, _} -> city end, fn {_, temp} -> temp end)
    |> Enum.into(%{}, fn {city, temps} -> {city, Stats.stats(temps)} end)
  end

  defp stream_file_concurrent(path, modes \\ [], min_chunk_bytes \\ 2 * @mib) do
    Stream.resource(
      fn -> File.open!(path, modes) end,
      fn file ->
        case IO.read(file, min_chunk_bytes) do
          :eof ->
            {:halt, file}

          chunk ->
            case IO.read(file, :line) do
              :eof -> {[chunk], file}
              rest -> {[chunk <> rest], file}
            end
        end
      end,
      &File.close/1
    )
    |> Task.async_stream(&split_lines/1)
    |> Stream.map(fn {:ok, i} -> i end)
  end

  defp group_by(enum, key_fn, val_fn), do: :maps.groups_from_list(key_fn, val_fn, enum)

  defp split_lines(chunk), do: :binary.split(chunk, <<"\n">>, [:global]) |> process_lines2()

  defp parse_line_binary(line) do
    case line do
      "" ->
        {line, 0}

      line ->
        [city, temp_bin] = :binary.split(line, <<";">>)
        {city, :erlang.binary_to_float(temp_bin)}
    end
  end

  defp parse_line_binary2(line) do
    case line do
      "" ->
        {line, 0}

      line ->
        [city, temp_bin] = :binary.split(line, ";")
        {city, :erlang.binary_to_float(temp_bin)}
    end
  end

  defmodule Stats do
    @neutral {Float.max_finite(), Float.min_finite(), 0, 0}

    def format({k, {mn, mx, sum, cnt}}), do: "#{k};#{mn};#{Float.round(sum / cnt, 1)};#{mx}"

    def stats(t), do: {:lists.min(t), :lists.max(t), :lists.sum(t), length(t)}

    def stats2(temps), do: Enum.reduce(temps, @neutral, &stats_reducer/2)

    def stats_reducer(v, {mn, mx, sum, cnt}), do: {min(v, mn), max(v, mx), sum + v, cnt + 1}

    def merge_stats(_k, {min1, max1, total1, len1}, {min2, max2, total2, len2}) do
      {min(min1, min2), max(max1, max2), total1 + total2, len1 + len2}
    end
  end
end
