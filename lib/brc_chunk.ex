defmodule Brc.Chunk do
  @neutral {Float.max_finite(), Float.min_finite(), 0, 0}

  @mib 2 ** 20
  def stream_file_concurrent(path, concurrent_mapper, modes \\ [], min_chunk_bytes \\ 2 * @mib) do
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
      fn file -> File.close(file) end
    )
    |> Task.async_stream(fn chunk -> String.split(chunk, "\n") |> concurrent_mapper.() end)
    |> Stream.map(fn {:ok, i} -> i end)
  end

  defp stats(temps), do: Enum.reduce(temps, @neutral, &stats_reducer/2)

  defp stats_reducer(v, {mn, mx, sum, cnt}), do: {min(v, mn), max(v, mx), sum + v, cnt + 1}

  defp merge_stats(_k, {min1, max1, total1, len1}, {min2, max2, total2, len2}) do
    { min(min1, min2), max(max1, max2), total1 + total2, len1 + len2 }
  end

  def process_lines(lines) do
    lines
    |> Stream.map(&Regex.run(~r/^([^;]+);(.*)$/, &1))
    |> Stream.reject(&Kernel.is_nil/1)
    |> Stream.map(fn [_, city, temp] -> {city, elem(Float.parse(temp), 0)} end)
    |> Enum.group_by(fn {city, _} -> city end, fn {_, temp} -> temp end)
    |> Enum.into(%{}, fn {city, temps} -> {city, stats(temps)} end)
  end

  def process_file(path) do
    path
    |> stream_file_concurrent(&process_lines/1, [], 4 * @mib)
    |> Enum.reduce(%{}, fn x, map -> Map.merge(map, x, &merge_stats/3) end)
    |> Stream.map(fn {k, {mn, mx, sum, cnt}} -> "#{k};#{mn};#{Float.round(sum/cnt,1)};#{mx}" end)
    |> Enum.sort()
  end
end