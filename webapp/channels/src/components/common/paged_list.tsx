// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

import debounce from 'lodash/debounce';
import React, {forwardRef, useCallback, useEffect, useImperativeHandle, useMemo, useRef} from 'react';

import Scrollbars from 'components/common/scrollbars';
import LoadingSpinner from 'components/widgets/loading/loading_spinner';

const LOAD_MORE_BUFFER = 30;
const LOAD_MORE_DEBOUNCE = 100;

type Props = {
    children: React.ReactNode;

    /** Whether another page can be loaded. The loading spinner is shown at the end of the list while true. */
    hasMore: boolean;

    /** Whether the next page is being fetched, which holds off further requests. */
    isLoadingMore: boolean;

    onLoadMore: () => void;

    /** Keep loading pages until the list overflows the viewport, for lists whose pages can come back short or empty. */
    loadUntilFull?: boolean;
};

const PagedList = forwardRef<HTMLDivElement, Props>(({
    children,
    hasMore,
    isLoadingMore,
    onLoadMore,
    loadUntilFull = false,
}, ref) => {
    const scrollbars = useRef<HTMLDivElement>(null);
    useImperativeHandle(ref, () => scrollbars.current as HTMLDivElement, []);
    const loader = useRef<HTMLDivElement>(null);

    const onLoadMoreRef = useRef(onLoadMore);
    onLoadMoreRef.current = onLoadMore;

    const loadMore = useMemo(() => debounce(() => onLoadMoreRef.current(), LOAD_MORE_DEBOUNCE), []);
    useEffect(() => () => loadMore.cancel(), [loadMore]);

    // Measured from the top of the loading spinner, so that seeing the spinner is enough to load the next page.
    const loadMoreIfNearEnd = () => {
        if (!hasMore || isLoadingMore || !scrollbars.current) {
            return;
        }

        const {scrollTop, clientHeight, scrollHeight} = scrollbars.current;
        const loaderHeight = loader.current?.offsetHeight ?? 0;
        if (scrollTop + clientHeight + LOAD_MORE_BUFFER >= scrollHeight - loaderHeight) {
            loadMore();
        }
    };
    const loadMoreIfNearEndRef = useRef(loadMoreIfNearEnd);
    loadMoreIfNearEndRef.current = loadMoreIfNearEnd;
    const onScroll = useCallback(() => loadMoreIfNearEndRef.current(), []);

    // Checking on resize rather than on every render means a failed page isn't retried until the list or the
    // viewport changes size, or the user scrolls.
    useEffect(() => {
        const scrollable = scrollbars.current;
        if (!loadUntilFull || !scrollable) {
            return undefined;
        }

        const observer = new ResizeObserver(() => loadMoreIfNearEndRef.current());
        observer.observe(scrollable);
        for (const content of Array.from(scrollable.children)) {
            observer.observe(content);
        }

        return () => observer.disconnect();
    }, [loadUntilFull]);

    return (
        <Scrollbars
            ref={scrollbars}
            color='--center-channel-color-rgb'
            onScroll={onScroll}
        >
            {children}
            {hasMore && (
                <div
                    ref={loader}
                    className='loading-screen'
                >
                    <div className='loading__content'>
                        <LoadingSpinner/>
                    </div>
                </div>
            )}
        </Scrollbars>
    );
});
PagedList.displayName = 'PagedList';

export default PagedList;
