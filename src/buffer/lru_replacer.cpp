//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub
{

    LRUReplacer::LRUReplacer(size_t num_pages) : capacity{num_pages} {}

    LRUReplacer::~LRUReplacer() = default;

    /*Remove the object that was accessed the least recently compared
    to all the elements being tracked by the Replacer, store its contents
    in the output parameter and return True. If the Replacer is empty return
    False*/
    bool LRUReplacer::Victim(frame_id_t *frame_id)
    {
        const std::lock_guard<mutex_t> guard(mutex);

        if (frames.empty())
            return false;

        auto frame = frames_map.find(frames.front());

        if (frame != frames_map.end())
        {
            frames_map.erase(frame);
            *frame_id = frames.front();
            frames.pop_back();
            return true;
        }
        else
            return false;
    }

    void LRUReplacer::Pin(frame_id_t frame_id)
    {
        /*This method should be called after a page is pinned to a frame in the
         BufferPoolManager. It should remove the frame containing the pinned page
         from the LRUReplacer.*/

        const std::lock_guard<mutex_t> guard(mutex);

        auto frame = frames_map.find(frame_id);
        if (frame != frames_map.end())
        {
            frames.erase(frame->second);
            frames_map.erase(frame);
        }
    }

    void LRUReplacer::Unpin(frame_id_t frame_id)
    {
        /*This method should be called when the pin_count of a page becomes 0.
         This method should add the frame containing the unpinned page to the
         LRUReplacer*/
        const std::lock_guard<mutex_t> guard(mutex);

        auto frame=frames_map.find(frame_id);
        if(frame==frames_map.end())
        return;
        if(frames_map.size()>=capacity)
        return;
        frames.push_back(frame_id);
        auto it=frames.end();
        it--;
        frames_map.insert({frame_id,it});

    }

    size_t LRUReplacer::Size()
    {
        const std::lock_guard<mutex_t> guard(mutex);
        return frames_map.size();
    }

} // namespace bustub
