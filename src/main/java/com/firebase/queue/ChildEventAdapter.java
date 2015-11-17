package com.firebase.queue;

import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.FirebaseError;

/*package*/ class ChildEventAdapter implements ChildEventListener {
  @Override
  public void onChildAdded(DataSnapshot dataSnapshot, String previousChildKey) {}

  @Override
  public void onChildChanged(DataSnapshot dataSnapshot, String previousChildKey) {}

  @Override
  public void onChildRemoved(DataSnapshot dataSnapshot) {}

  @Override
  public void onChildMoved(DataSnapshot dataSnapshot, String previousChildKey) {}

  @Override
  public void onCancelled(FirebaseError firebaseError) {}
}
