package fi.tkk.ics.jbliss;

import com.koloboke.collect.map.hash.HashIntIntMap;

/**
 * An interface for reporting the found generator automorphisms.
 */
public interface Reporter
{
    /**
     * The hook method that is called when a new generator automorphism
     * is found.
     *
     * @param aut         An automorphism
     * @param user_param  A parameter provided by the user
     */
    public void report(HashIntIntMap aut, Object user_param);
}
