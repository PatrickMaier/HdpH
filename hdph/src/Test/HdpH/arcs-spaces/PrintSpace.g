# Explicit representation of finite geometries in 2 dimensions

LoadPackage("fining");


PrintSpace := function(space, file)
  local points, lines, out, l, a;

  points := AsSet(Points(space));
  lines  := AsSet(Lines(space));

  if IsEmpty(file) then
    out := OutputTextUser();
  else
    out := OutputTextFile(file, false);
  fi;

  AppendTo(out, "c ", space, "\n");
  AppendTo(out, "d ", Size(points), " #points\n");
  AppendTo(out, "d ", Size(AsSet(Points(lines[1]))), " #points/line\n");
  AppendTo(out, "d ", Size(lines), " #lines\n");
  AppendTo(out, "d ", Size(AsSet(Lines(points[1]))), " #lines/point\n");

  for l in lines do
    AppendTo(out, "l");
    for a in AsSet(Points(l)) do
      AppendTo(out, " ", Position(points, a));
    od;
    AppendTo(out, "\n");
  od;

  CloseStream(out);
end;;

#t0 := Runtime();
#for q in [2, 3, 4, 5, 7, 8, 9, 11, 13, 16, 17, 19, 23, 25, 27, 29, 31, 32] do
#  file := Concatenation("PG_2_", String(q), ".txt");
#  PrintSpace(PG(2,q), file);
#od;
#t1 := Runtime();
#Print("\nSmall spaces: ", t1 - t0, " ms\n");

#t0 := Runtime();
#for q in [ 37, 41, 43, 47, 49, 53, 59, 61, 64, 67, 71, 73, 79, 81, 83] do
#  file := Concatenation("PG_2_", String(q), ".txt");
#  PrintSpace(PG(2,q), file);
#od;
#t1 := Runtime();
#Print("\nBigger spaces: ", t1 - t0, " ms\n");

#QUIT;
