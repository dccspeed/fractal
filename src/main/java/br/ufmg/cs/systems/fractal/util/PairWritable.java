package br.ufmg.cs.systems.fractal.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Pair Writable class, that knows types upfront and can deserialize itself.
 *
 * PairWritable knows types, as instances are passed through constructor, and
 * are references are immutable (values themselves are mutable).
 *
 * Child classes specify no-arg constructor that passes concrete types in.
 *
 * Extends Pair, not ImmutablePair, since later class is final. Code is
 * copied from it.
 *
 * @param <L> Type of the left element
 * @param <R> Type of the right element
 */
public class PairWritable<L extends Writable, R extends Writable>
    extends Pair<L, R> implements Writable {
  /** Left object */
  private L left;
  /** Right object */
  private R right;

  public PairWritable() {
  }

  /**
   * Create a new pair instance.
   *
   * @param left  the left value
   * @param right  the right value
   */
  public PairWritable(L left, R right) {
    this.left = left;
    this.right = right;
  }

  /**
   * <p>
   * Obtains an immutable pair of from two objects inferring
   * the generic types.</p>
   *
   * <p>This factory allows the pair to be created using inference to
   * obtain the generic types.</p>
   *
   * @param <L> the left element type
   * @param <R> the right element type
   * @param left  the left element, may be null
   * @param right  the right element, may be null
   * @return a pair formed from the two parameters, not null
   */
  public static <L extends Writable, R extends Writable>
  PairWritable<L, R> of(L left, R right) {
    return new PairWritable<L, R>(left, right);
  }

  @Override
  public final L getLeft() {
    return left;
  }

  @Override
  public final R getRight() {
    return right;
  }

  /**
   * <p>Throws {@code UnsupportedOperationException}.</p>
   *
   * <p>This pair is immutable, so this operation is not supported.</p>
   *
   * @param value  the value to set
   * @return never
   * @throws UnsupportedOperationException as this operation is not supported
   */
  @Override
  public final R setValue(R value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    Text.writeString(out, left.getClass().getName());
    Text.writeString(out, right.getClass().getName());
    left.write(out);
    right.write(out);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    try {
       Class<? extends Writable> lclass = Class.forName(Text.readString(in)).
	   asSubclass(Writable.class);
       Class<? extends Writable> rclass = Class.forName(Text.readString(in)).
	   asSubclass(Writable.class);
       left = (L) lclass.newInstance();
       right = (R) rclass.newInstance();
       left.readFields(in);
       right.readFields(in);
    } catch (InstantiationException e) {
	throw new IOException("Failed tuple init", e);
    } catch (ClassNotFoundException e) {
	throw new IOException("Failed tuple init", e);
    } catch (IllegalAccessException e) {
	throw new IOException("Failed tuple init", e);
    }
  }
}
