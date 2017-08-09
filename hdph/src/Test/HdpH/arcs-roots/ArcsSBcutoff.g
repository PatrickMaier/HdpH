##
## Enumerating arcs in PG(2,q), with orbital symmetry breaking and
## search tree cutoff based on size of candidates set.
##
## Refers to Open Problem 4.10(f) in
##   Hirschfeld, Thas. Open Problems in Finite Projective Spaces. 2015.
##

LoadPackage("fining");


# Comparison function for sorting sets or lists by size in descending order.
ByDescSize := function(x, y) return Size(x) > Size(y); end;;


## Convert list of points 'as' into the list of positions in set 'points'.
ToPos := function(points, as)
  local a;

  return List(as, a -> Position(points, a));
end;;


## Given a projective space 'ps' and a set 'as' of points in 'ps', return
## 'true' iff 'as' is an arc (i.e. no 3 points of 'as' are collinear in 'ps').
## NOTE: Inefficient test predicate.
IsArc := function(ps, as)
  local ab;

  ## No 3 points collinear iff there is no line arising from any pair of
  ## points in as that intersects with any other points in as.
  for ab in Combinations(as, 2) do
    if Size(Intersection(as, AsSet(Points(ps, Span(ab))))) > 2 then
      return false;
    fi;
  od;
  return true;
end;;


## Given a projective space 'ps', an arc 'arc' and point 'b' not in 'arc',
## return 'true' iff 'arc' can be extended with 'b' (i.e. 'Union(arc, [b])'
## is still an arc).
IsCandidateForArc := function(ps, arc, b)
  local l;

  ## 'b' is candidate iff each line through 'b' intersects <= 1 point of 'arc'
  for l in Lines(ps, b) do
    if Size(Intersection(arc, AsSet(Points(ps, l)))) > 1 then
      return false;
    fi;
  od;
  return true;
end;;


## Given a projective space 'ps' and an arc 'arc', return 'true' iff 'arc'
## is complete (i.e. it cannot be extended any further).
IsCompleteArc := function(ps, arc)
  local cands, b;

  cands := Set(Points(ps));
  SubtractSet(cands, arc);
  return ForAll(cands, b -> not IsCandidateForArc(ps, arc, b));
end;;


## A node (in the arc search tree) is a record storing a projective space,
## an arc and set of candidate points for extending the arc.
## Given a record 'r', return 'true' iff 'r' is a search tree node.
## NOTE: Inefficient test predicate.
## TODO: Check fields k and pts
IsNode := function(r)
  local b;

  ## check record structure
  if not IsRecord(r) then return false; fi;
  if not IsBound(r.ps)    then return false; fi;
  if not IsBound(r.arc)   then return false; fi;
  if not IsBound(r.cands) then return false; fi;
  if not IsBound(r.stab)  then return false; fi;

  ## check field types
  if not IsProjectiveSpace(r.ps) then return false; fi;
  if not (IsSet(r.arc) and IsMutable(r.arc)) then return false; fi;
  if not (IsSet(r.cands) and IsMutable(r.cands)) then return false; fi;
  if r.stab <> [] and not (IsGroup(r.stab)) then return false; fi;

  ## check disjointness, arc property, candidate property and stability
  if not IsEmpty(Intersection(r.arc, r.cands)) then return false; fi;
  if not IsArc(r.ps, r.arc) then return false; fi;
  if ForAny(r.cands, b -> not IsCandidateForArc(r.ps, r.arc, b)) then
    return false;
  fi;
  if not IsEmpty(r.arc) and r.stab <> [] then
    if FiningOrbit(r.stab, r.arc, OnSetsProjSubspaces)[1] <> r.arc then
      return false;
    fi;
  fi;

  ## all tests passed
  return true;
end;;


# Comparison function for sorting nodes by size of 'cands' in descending order,
# in case of a tie by size of 'arc' in ascending order.
ByDescCandsAscArc := function(x, y)
  if Size(x.cands) = Size(y.cands) then
    return Size(x.arc) < Size(y.arc);
  else
    return Size(x.cands) > Size(y.cands);
  fi;
end;;


## Given a projective space 'ps' and positive integer 'k',
## return the root node of the 'k'-arc search tree.
RootNode := function(ps, k)
  return rec( ps    := ps,                      # projective space
              pts   := AsSet(Points(ps)),       # set of points of space 'ps'
              k     := k,                       # target arc size
              arc   := [],                      # arc (empty at root)
              cands := Set(Points(ps)),         # candidates for arc
              stab  := CollineationGroup(ps) ); # some stabiliser of arc, or []
end;;


## Given a search tree node 'node' and a point 'c' which is a candidate for
## extending 'node.arc' but is not in 'node.cands', shrink 'node.cands' such
## that it is a set of candidates when 'node.arc' is extended with 'c'.
## Returns nothing but mutates 'node.cands' as a side effect.
ShrinkCandsOnExtendingArc := function(node, c)
  local l, a;

  # iterate over all lines incident to c
  for l in Lines(node.ps, c) do
    # select lines l that are incident to a point in node.arc;
    # as c is a candidate, each of its lines is incident to at most 1 point
    if ForAny(node.arc, a -> a in l) then
      # remove all points on those lines from node.cands
      SubtractSet(node.cands, List(Points(node.ps, l)));
    fi;
  od;
end;


## Given a search tree node 'node', return a list of children of 'node'
## in the search tree when enumerating the 'node.k'-arcs.
## 'cutoff' is a lower bound on the number of candidates; the function stops
## generating children as soon as the number candidates drops below 'cutoff'.
## Assumes that 'node.stab' is not [] and stops tree exploration
## as soon as 'node.stab' becomes trivial.
GenerateNodes := function(node, cutoff)
  local orbits, orb, c, children, child, candidates, setstab;

  ## return no children if less than 'cutoff' candidates
  if Size(node.cands) < cutoff then return []; fi;

  children   := [];
  candidates := Set(node.cands);

  ## partition 'candidates' into 'orbits' via pointwise stabiliser
  orbits := List(FiningOrbits(node.stab, candidates, OnProjSubspaces),
                 orb -> Intersection(orb, candidates));
#Print("\n|orbit|=", List(orbits, Size), "\n"); #DEBUG

  ## if orbit not transitive refine 'node.stab' to setwise stabiliser
  if Size(orbits) > 1 then

    ## compute setwise stabiliser and partition 'candidates' into 'orbits'
    node.stab := FiningSetwiseStabiliser(CollineationGroup(node.ps), node.arc);
    orbits := List(FiningOrbits(node.stab, candidates, OnProjSubspaces),
                   orb -> Intersection(orb, candidates));
#Print("\n|ORBIT|=", List(orbits, Size), "\n"); #DEBUG
  fi;

  ## return no children if stabiliser is trivial (and set 'node.stab')
  if IsTrivial(node.stab) then
    node.stab := [];
    return [];
  fi;

  ## iterate over 'orbits', sorted by size (biggest orbit first)
  Sort(orbits, ByDescSize);
  for orb in orbits do

    ## prune "to the right" if set of candidates too small to reach k-arc
    if Size(node.arc) + Size(candidates) < node.k then
      return children;
    fi;

    ## stop generation if number of candidates drops below 'cutoff'
    if Size(candidates) < cutoff then

      ## adjust candidate set of current node
      node.cands := candidates;
      return children;
    fi;

    ## pick a representative 'c' from 'orb' and remove it from 'candidates'
    c := Representative(orb);
    RemoveSet(candidates, c);

    ## construct 'child' by extending arc with 'c' and adjusting cands
    child := rec( ps    := node.ps,
                  pts   := node.pts,
                  k     := node.k,
                  arc   := Set(node.arc),
                  cands := Set(candidates),
                  stab  := node.stab );
    ShrinkCandsOnExtendingArc(child, c);
    AddSet(child.arc, c);

    ## add child only if it has a chance of reaching a k-arc, else prune
    if Size(child.arc) + Size(child.cands) >= child.k then

      ## adjust stab to a pointwise stabiliser before adding 'child'
      child.stab := FiningStabiliserOrb(node.stab, c);
#Print("\narc=", ToPos(child.pts, child.arc), "\n"); #DEBUG
      Add(children, child);
    fi;

    ## remove whole orbit 'orb' from 'candidates'
    SubtractSet(candidates, orb);
  od;

  ## all children generated
  return children;
end;;


## Auxiliary function called by EnumerateSearchTree; returns nothing.
AccumulateSearchTreeFrom := function(cutoff, node, accu)
  local children, child;

  ## generate children
  children := GenerateNodes(node, cutoff);

  ## termination: trivial stabiliser or fewer than 'cutoff' candidates
  if node.stab = [] or Size(node.cands) < cutoff then
    Add(accu, node);
Print("\narc=", ToPos(node.pts, node.arc)); #DEBUG
Print("\t|cands|=", Size(node.cands));      #DEBUG
Print("\t|stab|=", Size(node.stab), "\n");  #DEBUG
  fi;

  ## recursive calls
  for child in children do
    AccumulateSearchTreeFrom(cutoff, child, accu);
  od;
end;;


## Given a projective space 'ps' a positive integer 'k' and a
## non-negative integer 'cutoff', return the frontier of search tree nodes
## such that the size of field 'cands' is less than 'cutoff'.
EnumerateSearchTree := function(ps, k, cutoff)
  local nodes, node;

  nodes := [];
  AccumulateSearchTreeFrom(cutoff, RootNode(ps, k), nodes);

  ## recompute setwise stabilisers
  for node in nodes do
    if IsEmpty(node.arc) then
      node.stab := CollineationGroup(ps);
    elif node.stab <> [] then
      node.stab := FiningSetwiseStabiliser(CollineationGroup(ps), node.arc);
      if IsTrivial(node.stab) then node.stab := []; fi;
    fi;
  od;

  ## sort nodes in descending order of size of field 'cands'
  Sort(nodes, ByDescCandsAscArc);

  return nodes;
end;;


## Given a non-empty list 'nodes' of search tree nodes corresponding
## to one level of the tree, print arcs and candidates to 'file'.
PrintSearchTreeNodes := function(nodes, file)
  local out, space, points, k, node, a, c, i;

  space  := nodes[1].ps;
  points := nodes[1].pts;
  k      := nodes[1].k;

  if IsEmpty(file) then
    out := OutputTextUser();
  else
    out := OutputTextFile(file, false);
  fi;

  AppendTo(out, "c root arcs for all ", k, "-arcs in ", space, "\n");

  i := 0;
  for node in nodes do
    AppendTo(out, "\n");
    i := i + 1;

    # write comment (root #, size of stabiliser, number of candidates)
    AppendTo(out, "c #", i);
    AppendTo(out, " |arc|=", Size(node.arc));
    AppendTo(out, " |stab|=", Size(node.stab));
    AppendTo(out, " |candidates|=", Size(node.cands), "\n");

    # write arc
    AppendTo(out, "a");  ## prefix "a" stands for "arc"
    for a in node.arc do
      AppendTo(out, " ", Position(points, a));
    od;
    AppendTo(out, "\n");

    # write candidate set
    AppendTo(out, "r");  ## prefix "r" stands "remaining candidates"
    for c in node.cands do
      AppendTo(out, " ", Position(points, c));
    od;
    AppendTo(out, "\n");
  od;

  CloseStream(out);
end;;


## Generating roots for (q-1)-arcs in PG(2,q), 7 <= q <= 83, cutoff 400.
## NB: Cannot do q=p^2 because prime squares trigger a bug in SetwiseStabiliser.
cutoff := 400;
ps := Filtered(Primes, p -> p <= 83);
qs := Concatenation(List([1, 3, 4, 5, 6], i ->
        Filtered(List(ps, p -> p^i), q -> 7 <= q and q <= 83)));
Sort(qs);
#for q in qs do
#  file := Concatenation("PG_2_", String(q), "_cutoff", String(cutoff), ".txt");
#  t0 := Runtime();;
#  nodes := EnumerateSearchTree(PG(2,q), q - 1, cutoff);;
#  t1 := Runtime();;
#  Print("\nq=", q, " roots = ", Length(nodes), " {", t1 - t0, "ms}");
#  Print("\nq=", q, " |arc| = ", Set(nodes, node -> Size(node.arc)));
#  Print("\nq=", q, " |stab| = ", Set(nodes, node -> Size(node.stab)), "\n");
#  PrintSearchTreeNodes(nodes, file);
#  Print("\n");
#od;


#QUIT;
