package com.scalar.db.common;

public interface TwoPhaseCommitTransactionDecoratorAddable {

  void addTransactionDecorator(TwoPhaseCommitTransactionDecorator transactionDecorator);
}
